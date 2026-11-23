// Package router decides which deployment serves a request.
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
	ID              string
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

func (g *Group) ObserveOutput(tokens int) {
	old := math.Float64frombits(g.avgOut.Load())
	v := float64(tokens)
	if old != 0 {
		v = old*0.95 + v*0.05
	}
	g.avgOut.Store(math.Float64bits(v))
}

func (g *Group) expectedOutput(req *llm.ChatRequest) int {
	avg := int(math.Float64frombits(g.avgOut.Load()))
	if avg == 0 {
		avg = 256
	}
	if req.MaxTokens != nil && *req.MaxTokens > 0 && *req.MaxTokens < avg {
		return *req.MaxTokens
	}
	return avg
}

type Policy struct {
	Strategy      string
	LatencyTarget time.Duration
	Prefer        string // sticky deployment id; ignored if its breaker is open
}

type Router struct {
	groups  map[string]*Group
	Budget  *Budget
	explore float64
}

var ErrNoHealthy = errors.New("no healthy deployment")

func New(groups map[string]*Group, budget *Budget) *Router {
	for _, g := range groups {
		sort.SliceStable(g.Deployments, func(i, j int) bool {
			return g.Deployments[i].Priority < g.Deployments[j].Priority
		})
	}
	// small random share so a deployment that once looked slow isn't starved forever
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
	score     float64
	cost      float64
}

func (r *Router) Plan(g *Group, p Policy, req *llm.ChatRequest) []*Deployment {
	now := time.Now()
	expOut := g.expectedOutput(req)
	inTok := estimateInputTokens(req)

	cands := make([]candidate, 0, len(g.Deployments))
	for _, d := range g.Deployments {
		if !d.Breaker.Peek(now) {
			continue
		}
		snap := d.Stats.Snapshot(now)
		pred, known := snap.Predict(expOut)
		score := float64(pred) / (1 - min(snap.ErrorRate, 0.9)) // expected time incl. retries at this error rate
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
	default:
	}

	// applied last so stickiness overrides strategy order, not failover order
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

// unknown latency sorts first (score 0) so new/recovered deployments get traffic
func rankFastest(c []candidate) {
	sort.SliceStable(c, func(i, j int) bool { return c[i].score < c[j].score })
}

// cheapest among those meeting target, then fastest; target 0 = pure cheapest
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

// u is uniform in [0,1); rest stay in priority order as fallbacks
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

// ~4 chars/token; used only for ranking, where the error applies to every candidate equally
func estimateInputTokens(req *llm.ChatRequest) int {
	n := 0
	for _, m := range req.Messages {
		n += len(m.Content)
	}
	return n/4 + 1
}

func Classify(err error) Outcome {
	if err == nil {
		return Success
	}
	if llm.AsError(err).CountsAsFailure() {
		return Failure
	}
	return Neutral
}

func (r *Router) Record(d *Deployment, probe bool, err error) {
	now := time.Now()
	o := Classify(err)
	if o != Neutral {
		d.Stats.addResult(o == Success, now)
	}
	n, fails := d.Stats.counts(now)
	from, to := d.Breaker.Report(now, probe, o, n, fails)
	if from == HalfOpen && to == Closed { // clear stale outage failures, else the next one re-trips
		d.Stats.resetErrors()
	}
}

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
