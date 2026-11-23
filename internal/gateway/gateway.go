// Package gateway executes the router's plan: attempts, failover, hedging.
package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"time"

	"llmgateway/internal/cache"
	"llmgateway/internal/llm"
	"llmgateway/internal/metrics"
	"llmgateway/internal/provider"
	"llmgateway/internal/router"
)

var (
	ErrUnknownModel = errors.New("unknown model")
	ErrNoHealthy    = router.ErrNoHealthy
)

type Gateway struct {
	r        *router.Router
	cache    *cache.Cache // nil = disabled
	sessions SessionStore // nil = sticky routing disabled
	m        *metrics.Metrics
	log      *slog.Logger
}

func New(r *router.Router, c *cache.Cache, s SessionStore, m *metrics.Metrics, log *slog.Logger) *Gateway {
	return &Gateway{r: r, cache: c, sessions: s, m: m, log: log}
}

func (g *Gateway) Models() []string { return g.r.GroupNames() }

func (g *Gateway) Status() []router.DeploymentStatus { return g.r.Status() }

type Options struct {
	Policy  router.Policy
	NoCache bool
	Session string // client's conversation id, scoped by API key; "" = no sticky routing
}

type Result struct {
	Response   *llm.ChatResponse
	Deployment *router.Deployment // nil only for cache hits whose deployment was removed
	DeployID   string
	Attempts   int
	Hedged     bool
	CacheHit   bool
	Upstream   time.Duration // wall time spent waiting on providers
	Sticky     string        // "", new, hit, moved (see stickyUpdate)
}

// Failure wraps an upstream error with the attempt metadata collected before
// it gave up, so failed-request logs/headers show which deployments were tried.
type Failure struct {
	Err        error
	Attempts   int
	Deployment *router.Deployment // last (or only) deployment tried
	DeployID   string
	Hedged     bool
	Upstream   time.Duration
}

func (f *Failure) Error() string { return f.Err.Error() }
func (f *Failure) Unwrap() error { return f.Err }

func withAttempts(err error, attempts int, d *router.Deployment, hedged bool, upstream time.Duration) error {
	if attempts == 0 || err == nil {
		return err
	}
	deployID := ""
	if d != nil {
		deployID = d.ID
	}
	return &Failure{Err: err, Attempts: attempts, Deployment: d, DeployID: deployID, Hedged: hedged, Upstream: upstream}
}

func (g *Gateway) Chat(ctx context.Context, req *llm.ChatRequest, opt Options) (*Result, error) {
	grp, ok := g.r.Group(req.Model)
	if !ok {
		return nil, ErrUnknownModel
	}

	var key string
	cacheable, cacheErr := false, false
	if g.cache != nil && !opt.NoCache {
		key, cacheable = g.cache.Key(req)
	}
	if !cacheable {
		g.m.Cache.WithLabelValues("skip").Inc()
	} else {
		e, hit, err := g.cache.Get(ctx, key)
		switch {
		case err != nil:
			g.m.Cache.WithLabelValues("error").Inc() // fail open: treat as miss
			cacheErr = true
		case hit:
			g.m.Cache.WithLabelValues("hit").Inc()
			resp := e.Response
			return &Result{Response: &resp, DeployID: e.Deployment, CacheHit: true}, nil
		default:
			g.m.Cache.WithLabelValues("miss").Inc()
		}
	}

	res, err := g.chatUpstream(ctx, grp, req, opt)
	if err != nil {
		return nil, err
	}
	if cacheable && !cacheErr && res.Response.FinishReason == "stop" { // don't cache truncated answers
		if err := g.cache.Set(ctx, key, &cache.Entry{Response: *res.Response, Deployment: res.DeployID}); err != nil {
			g.m.Cache.WithLabelValues("error").Inc()
		}
	}
	return res, nil
}

type attemptResult struct {
	d     *router.Deployment
	probe bool
	resp  *llm.ChatResponse
	err   error
	dur   time.Duration
}

// at most two attempts in flight at once (primary + one hedge); first success wins
func (g *Gateway) chatUpstream(ctx context.Context, grp *router.Group, req *llm.ChatRequest, opt Options) (*Result, error) {
	now := time.Now()
	g.r.Budget.OnRequest(now)
	pol := g.r.Resolve(grp, opt.Policy)
	skey, prefer := g.stickyLookup(ctx, grp.Name, opt)
	pol.Prefer = prefer
	plan := g.r.Plan(grp, pol, req)
	if len(plan) == 0 {
		return nil, ErrNoHealthy
	}

	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll() // cancels a losing hedge as soon as we return

	results := make(chan attemptResult, len(plan))
	start := time.Now()
	next, inflight, attempts := 0, 0, 0
	hedged := false
	var lastErr error
	var lastDeploy *router.Deployment
	var firstDeploy *router.Deployment // launch() can skip plan[0] on breaker denial
	prevOpen := false

	defer func() { // drain the loser so a held half-open probe slot isn't leaked
		if n := inflight; n > 0 {
			go func() {
				for range n {
					g.recordAttempt(<-results, ctx)
				}
			}()
		}
	}()

	launch := func(isRetry bool) bool {
		for next < len(plan) {
			d := plan[next]
			next++
			probe, ok := d.Breaker.Allow(time.Now())
			if !ok {
				continue
			}
			if isRetry && !prevOpen { // failover onto an open breaker is free
				if !g.r.Budget.TryRetry(time.Now()) {
					g.m.RetryBudget.WithLabelValues("exhausted").Inc()
					if probe { // give the probe slot back
						g.r.Record(d, true, &llm.Error{Kind: llm.KindCanceled})
					}
					next = len(plan)
					return false
				}
				g.m.RetryBudget.WithLabelValues("allowed").Inc()
			}
			attempts++
			inflight++
			if !isRetry {
				firstDeploy = d
			}
			go func() {
				d.Stats.IncInFlight()
				t0 := time.Now()
				resp, err := d.Provider.Chat(ctx, d.Model, req)
				d.Stats.DecInFlight()
				results <- attemptResult{d: d, probe: probe, resp: resp, err: err, dur: time.Since(t0)}
			}()
			return true
		}
		return false
	}

	if !launch(false) {
		return nil, ErrNoHealthy // no deployment's breaker admitted a request: nothing was attempted
	}

	var hedgeC <-chan time.Time
	if grp.HedgeAfter > 0 && len(plan) > 1 {
		t := time.NewTimer(grp.HedgeAfter)
		defer t.Stop()
		hedgeC = t.C
	}

	for inflight > 0 {
		select {
		case <-hedgeC:
			hedgeC = nil
			prevOpen = false // hedge is budgeted like a retry
			if launch(true) {
				hedged = true
				g.m.Hedges.WithLabelValues("started").Inc()
			}
		case ar := <-results:
			inflight--
			g.recordAttempt(ar, ctx)
			if ar.err == nil {
				if hedged && ar.d != firstDeploy {
					g.m.Hedges.WithLabelValues("won").Inc()
				}
				grp.ObserveOutput(ar.resp.Usage.OutputTokens)
				upstream := time.Since(start)
				sticky := g.stickyUpdate(ctx, skey, prefer, ar.d.ID)
				return &Result{Response: ar.resp, Deployment: ar.d, DeployID: ar.d.ID,
					Attempts: attempts, Hedged: hedged, Upstream: upstream, Sticky: sticky}, nil
			}
			lastErr = ar.err
			lastDeploy = ar.d
			e := llm.AsError(ar.err)
			if !e.Retryable() || ctx.Err() != nil {
				if inflight == 0 {
					return nil, withAttempts(ar.err, attempts, ar.d, hedged, time.Since(start))
				}
				continue
			}
			prevOpen = ar.d.Breaker.State() == router.Open
			if inflight == 0 && !launch(true) {
				return nil, withAttempts(lastErr, attempts, lastDeploy, hedged, time.Since(start))
			}
		}
	}
	return nil, withAttempts(lastErr, attempts, lastDeploy, hedged, time.Since(start))
}

func (g *Gateway) recordAttempt(ar attemptResult, ctx context.Context) {
	err := ar.err
	if err != nil && ctx.Err() != nil && llm.AsError(err).Kind != llm.KindTimeout {
		err = &llm.Error{Kind: llm.KindCanceled, Err: err}
	}
	g.r.Record(ar.d, ar.probe, err)
	if err == nil {
		ar.d.Stats.ObserveNonStream(ar.dur, ar.resp.Usage.OutputTokens)
		g.m.UpstreamTime.WithLabelValues(ar.d.ID).Observe(ar.dur.Seconds())
		g.m.Attempts.WithLabelValues(ar.d.ID, "ok").Inc()
		return
	}
	g.m.Attempts.WithLabelValues(ar.d.ID, llm.AsError(err).Kind.String()).Inc()
}

// Stream has already received its first chunk: failover only works before
// anything reaches the client, so we wait for it before sending headers.
type Stream struct {
	Deployment *router.Deployment
	Attempts   int
	TTFT       time.Duration // upstream time to first chunk, final attempt
	CacheHit   bool
	Sticky     string // "", new, hit, moved

	g        *Gateway
	grp      *router.Group
	s        provider.Stream
	probe    bool
	first    *llm.StreamChunk
	firstEOF bool
	firstAt  time.Time
	lastAt   time.Time
	outToks  int
	usage    *llm.Usage
	finished bool

	replay []llm.StreamChunk // cache hit: chunks to replay
}

func (st *Stream) Recv() (llm.StreamChunk, error) {
	if st.replay != nil {
		if len(st.replay) == 0 {
			return llm.StreamChunk{}, io.EOF
		}
		c := st.replay[0]
		st.replay = st.replay[1:]
		return c, nil
	}
	if st.first != nil {
		c := *st.first
		st.first = nil
		st.track(c)
		return c, nil
	}
	if st.firstEOF {
		st.finish(nil)
		return llm.StreamChunk{}, io.EOF
	}
	c, err := st.s.Recv()
	if errors.Is(err, io.EOF) {
		st.finish(nil)
		return c, err
	}
	if err != nil {
		st.finish(err)
		return c, err
	}
	st.track(c)
	return c, nil
}

func (st *Stream) track(c llm.StreamChunk) {
	if c.Delta != "" {
		st.outToks++ // ~1 chunk per token
		st.lastAt = time.Now()
	}
	if c.Usage != nil {
		st.usage = c.Usage
	}
}

func (st *Stream) finish(err error) {
	if st.finished || st.replay != nil {
		return
	}
	st.finished = true
	d := st.Deployment
	st.g.r.Record(d, st.probe, err)
	if err != nil {
		st.g.m.Attempts.WithLabelValues(d.ID, "midstream_"+llm.AsError(err).Kind.String()).Inc()
		return
	}
	toks := st.outToks
	if st.usage != nil && st.usage.OutputTokens > 0 {
		toks = st.usage.OutputTokens
	}
	if !st.lastAt.IsZero() {
		d.Stats.ObserveTPOT(st.lastAt.Sub(st.firstAt), toks)
	}
	st.grp.ObserveOutput(toks)
}

func (st *Stream) Close() error {
	if st.replay != nil {
		return nil
	}
	if !st.finished {
		st.finished = true
		st.g.r.Record(st.Deployment, st.probe, &llm.Error{Kind: llm.KindCanceled})
	}
	st.Deployment.Stats.DecInFlight()
	return st.s.Close()
}

func (g *Gateway) ChatStream(ctx context.Context, req *llm.ChatRequest, opt Options) (*Stream, error) {
	grp, ok := g.r.Group(req.Model)
	if !ok {
		return nil, ErrUnknownModel
	}
	if g.cache != nil && !opt.NoCache {
		if key, ok := g.cache.Key(req); ok {
			if e, hit, err := g.cache.Get(ctx, key); err == nil && hit {
				g.m.Cache.WithLabelValues("hit").Inc()
				return replayStream(g, grp, e), nil
			}
			g.m.Cache.WithLabelValues("miss").Inc()
		}
	}

	now := time.Now()
	g.r.Budget.OnRequest(now)
	pol := g.r.Resolve(grp, opt.Policy)
	skey, prefer := g.stickyLookup(ctx, grp.Name, opt)
	pol.Prefer = prefer
	plan := g.r.Plan(grp, pol, req)
	lastErr := ErrNoHealthy
	var lastDeploy *router.Deployment
	attempts := 0
	prevOpen := false
	for _, d := range plan {
		probe, ok := d.Breaker.Allow(time.Now())
		if !ok {
			continue
		}
		if attempts > 0 && !prevOpen {
			if !g.r.Budget.TryRetry(time.Now()) {
				g.m.RetryBudget.WithLabelValues("exhausted").Inc()
				if probe {
					g.r.Record(d, true, &llm.Error{Kind: llm.KindCanceled})
				}
				break
			}
			g.m.RetryBudget.WithLabelValues("allowed").Inc()
		}
		attempts++
		start := time.Now()
		d.Stats.IncInFlight()
		s, err := d.Provider.ChatStream(ctx, d.Model, req)
		if err == nil {
			first, rerr := s.Recv()
			if rerr == nil || errors.Is(rerr, io.EOF) {
				ttft := time.Since(start)
				d.Stats.ObserveTTFT(ttft)
				g.m.UpstreamTTFT.WithLabelValues(d.ID).Observe(ttft.Seconds())
				g.m.Attempts.WithLabelValues(d.ID, "ok").Inc()
				st := &Stream{Deployment: d, Attempts: attempts, TTFT: ttft, g: g, grp: grp,
					s: s, probe: probe, firstAt: time.Now()}
				if rerr == nil {
					st.first = &first
				} else {
					st.firstEOF = true
				}
				st.Sticky = g.stickyUpdate(ctx, skey, prefer, d.ID)
				return st, nil
			}
			_ = s.Close() // already failing over; the stream itself is discarded
			err = rerr
		}
		d.Stats.DecInFlight()
		g.r.Record(d, probe, err)
		g.m.Attempts.WithLabelValues(d.ID, llm.AsError(err).Kind.String()).Inc()
		lastErr = err
		lastDeploy = d
		if !llm.AsError(err).Retryable() || ctx.Err() != nil {
			break
		}
		prevOpen = d.Breaker.State() == router.Open
	}
	return nil, withAttempts(lastErr, attempts, lastDeploy, false, 0)
}

func replayStream(g *Gateway, grp *router.Group, e *cache.Entry) *Stream {
	r := e.Response
	var chunks []llm.StreamChunk
	text := r.Content
	for len(text) > 0 {
		n := 16
		if n > len(text) {
			n = len(text)
		}
		chunks = append(chunks, llm.StreamChunk{Delta: text[:n]})
		text = text[n:]
	}
	u := r.Usage
	chunks = append(chunks, llm.StreamChunk{FinishReason: r.FinishReason, Usage: &u})
	var d *router.Deployment
	for _, x := range grp.Deployments {
		if x.ID == e.Deployment {
			d = x
		}
	}
	return &Stream{Deployment: d, CacheHit: true, g: g, grp: grp, replay: chunks}
}

func (st *Stream) DeploymentID(fallback string) string {
	if st.Deployment != nil {
		return st.Deployment.ID
	}
	return fallback
}
