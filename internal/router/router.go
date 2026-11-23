// Package router decides which deployment serves a request. It owns the live
// state used for those decisions: per-deployment stats, circuit breakers and
// the global retry budget. It does not make network calls; the gateway package
// executes the plan the router returns.
package router

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"sort"
	"sync/atomic"
	"time"

	"llmgateway/internal/config"
	"llmgateway/internal/llm"
	"llmgateway/internal/provider"
)

type Deployment struct {
	ID              string // "provider/model"
	Group           string
	Provider        provider.Provider
	Model           string
	InputPricePerM  float64
	OutputPricePerM float64
	Priority        int
	Weight          int

	Stats   *Stats
	Breaker *Breaker
}

// Cost in USD for actual usage.
func (d *Deployment) Cost(u llm.Usage) float64 {
	return d.costFor(u.InputTokens, u.OutputTokens)
}

func (d *Deployment) costFor(in, out int) float64 {
	return float64(in)*d.InputPricePerM/1e6 + float64(out)*d.OutputPricePerM/1e6
}

type Group struct {
	Name          string
	Strategy      string
	LatencyTarget time.Duration
	HedgeAfter    time.Duration
	Deployments   []*Deployment

	avgOut atomic.Uint64 // float64 bits: EWMA of output tokens, for predictions
}

// ObserveOutput updates the group's typical answer length. A benign race
// (two concurrent updates, one lost) is fine for an average.
func (g *Group) ObserveOutput(tokens int) {
	old := math.Float64frombits(g.avgOut.Load())
	v := float64(tokens)
	if old != 0 {
		v = old*0.95 + v*0.05
	}
	g.avgOut.Store(math.Float64bits(v))
}

// expectedOutput guesses how many tokens this request will produce: the
// group's typical answer length, capped by max_tokens.
func (g *Group) expectedOutput(req *llm.ChatRequest) int {
	avg := int(math.Float64frombits(g.avgOut.Load()))
	if avg == 0 {
		avg = 256
	}
	if req.MaxTokens != nil && *req.MaxTokens < avg {
		return *req.MaxTokens
	}
	return avg
}

// Policy is the per-request routing choice: header > API key default > model default.
type Policy struct {
	Strategy      string
	LatencyTarget time.Duration
	// Prefer is a deployment id to try first (sticky routing): the one that
	// served this conversation's last turn. Ignored if that deployment's
	// breaker is open, so stickiness never overrides health.
	Prefer string
}

type Router struct {
	groups  map[string]*Group
	Budget  *Budget
	explore float64
}

// ErrNoHealthy means every deployment for the model has an open breaker.
// Failing fast is the point: waiting on known-dead providers helps nobody.
var ErrNoHealthy = errors.New("no healthy deployment")

func New(groups map[string]*Group, budget *Budget) *Router {
	for _, g := range groups {
		sort.SliceStable(g.Deployments, func(i, j int) bool {
			return g.Deployments[i].Priority < g.Deployments[j].Priority
		})
	}
	// 2% exploration: with "fastest"/"cheapest", a deployment that looked slow
	// once would otherwise never get traffic again, so its stats would never
	// update. A small random share keeps every deployment's data fresh.
	return &Router{groups: groups, Budget: budget, explore: 0.02}
}

func (r *Router) Group(name string) (*Group, bool) {
	g, ok := r.groups[name]
	return g, ok
}

func (r *Router) GroupNames() []string {
	names := make([]string, 0, len(r.groups))
	for n := range r.groups {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// Resolve fills in defaults from the group.
func (r *Router) Resolve(g *Group, p Policy) Policy {
	if p.Strategy == "" {
		p.Strategy = g.Strategy
	}
	if p.LatencyTarget == 0 {
		p.LatencyTarget = g.LatencyTarget
	}
	return p
}

type candidate struct {
	d         *Deployment
	predicted time.Duration
	known     bool
	score     float64 // predicted latency adjusted for error rate
	cost      float64 // expected USD for this request
}

// Plan returns deployments in the order they should be tried. Returning an
// ordered list (not one pick) makes failover just "walk the list".
func (r *Router) Plan(g *Group, p Policy, req *llm.ChatRequest) []*Deployment {
	now := time.Now()
	expOut := g.expectedOutput(req)
	inTok := estimateInputTokens(req)

	cands := make([]candidate, 0, len(g.Deployments))
	for _, d := range g.Deployments { // already in priority order
		if !d.Breaker.Peek(now) {
			continue
		}
		snap := d.Stats.Snapshot(now)
		pred, known := snap.Predict(expOut)
		// Expected time including failures: with error rate p, a request needs
		// 1/(1-p) attempts on average. A fast provider failing 50% of calls is
		// effectively twice as slow.
		score := float64(pred) / (1 - min(snap.ErrorRate, 0.9))
		cands = append(cands, candidate{d: d, predicted: pred, known: known, score: score,
			cost: d.costFor(inTok, expOut)})
	}

	switch p.Strategy {
	case config.StrategyFastest:
		rankFastest(cands)
		r.maybeExplore(cands)
	case config.StrategyCheapest:
		rankCheapest(cands, p.LatencyTarget)
		r.maybeExplore(cands)
	case config.StrategyWeighted:
		rankWeighted(cands, rand.Float64())
	default: // priority: already ordered
	}

	// Applied last, after strategy and exploration: a conversation stays on
	// its deployment even if the strategy would now rank another one first.
	// The rest of the plan keeps strategy order, so failover still works.
	if p.Prefer != "" {
		for i, c := range cands {
			if c.d.ID == p.Prefer {
				copy(cands[1:i+1], cands[:i])
				cands[0] = c
				break
			}
		}
	}

	out := make([]*Deployment, len(cands))
	for i, c := range cands {
		out[i] = c.d
	}
	return out
}

// Unknown latency sorts first (score 0): optimistic, so a new or recovered
// deployment gets traffic and quickly earns real stats.
func rankFastest(c []candidate) {
	sort.SliceStable(c, func(i, j int) bool { return c[i].score < c[j].score })
}

// rankCheapest: deployments predicted to meet the target, cheapest first;
// then the rest, fastest first, so a request still lands somewhere sensible
// when nothing meets the target. target 0 = pure cheapest.
func rankCheapest(c []candidate, target time.Duration) {
	meets := func(x candidate) bool {
		return target == 0 || !x.known || time.Duration(x.score) <= target
	}
	sort.SliceStable(c, func(i, j int) bool {
		mi, mj := meets(c[i]), meets(c[j])
		switch {
		case mi != mj:
			return mi
		case mi:
			if c[i].cost != c[j].cost {
				return c[i].cost < c[j].cost
			}
			return c[i].score < c[j].score
		default:
			return c[i].score < c[j].score
		}
	})
}

// rankWeighted picks the first deployment at random by weight; the rest stay
// in priority order as fallbacks. u is a uniform random number in [0,1).
func rankWeighted(c []candidate, u float64) {
	total := 0
	for _, x := range c {
		total += x.d.Weight
	}
	if total == 0 {
		return
	}
	pick := u * float64(total)
	for i, x := range c {
		pick -= float64(x.d.Weight)
		if pick < 0 {
			first := c[i]
			copy(c[1:i+1], c[:i])
			c[0] = first
			return
		}
	}
}

func (r *Router) maybeExplore(c []candidate) {
	if len(c) > 1 && rand.Float64() < r.explore {
		i := 1 + rand.IntN(len(c)-1)
		c[0], c[i] = c[i], c[0]
	}
}

// ~4 characters per token: rough, but only used to rank deployments by
// expected cost, where the same error applies to every candidate.
func estimateInputTokens(req *llm.ChatRequest) int {
	n := 0
	for _, m := range req.Messages {
		n += len(m.Content)
	}
	return n/4 + 1
}

// Classify maps an attempt error to a health outcome.
func Classify(err error) Outcome {
	if err == nil {
		return Success
	}
	if llm.AsError(err).CountsAsFailure() {
		return Failure
	}
	return Neutral
}

// Record feeds one attempt's outcome into stats and the breaker.
func (r *Router) Record(d *Deployment, probe bool, err error) {
	now := time.Now()
	o := Classify(err)
	if o != Neutral {
		d.Stats.addResult(o == Success, now)
	}
	n, fails := d.Stats.counts(now)
	d.Breaker.Report(now, probe, o, n, fails)
}

// DeploymentStatus is the admin/debug view of one deployment.
type DeploymentStatus struct {
	Group     string  `json:"group"`
	ID        string  `json:"id"`
	Breaker   string  `json:"breaker"`
	BaseP50Ms float64 `json:"base_p50_ms"`
	BaseP95Ms float64 `json:"base_p95_ms"`
	TPOTMs    float64 `json:"tpot_ms"`
	Samples   int     `json:"samples"`
	Requests  int     `json:"window_requests"`
	Failures  int     `json:"window_failures"`
	ErrorRate float64 `json:"error_rate"`
	InFlight  int64   `json:"in_flight"`
}

func (r *Router) Status() []DeploymentStatus {
	now := time.Now()
	var out []DeploymentStatus
	for _, name := range r.GroupNames() {
		for _, d := range r.groups[name].Deployments {
			s := d.Stats.Snapshot(now)
			ms := func(x time.Duration) float64 { return float64(x.Microseconds()) / 1000 }
			out = append(out, DeploymentStatus{
				Group: name, ID: d.ID, Breaker: d.Breaker.State().String(),
				BaseP50Ms: ms(s.BaseP50), BaseP95Ms: ms(s.BaseP95), TPOTMs: ms(s.TPOT),
				Samples: s.Samples, Requests: s.Requests, Failures: s.Failures,
				ErrorRate: s.ErrorRate, InFlight: s.InFlight,
			})
		}
	}
	return out
}
