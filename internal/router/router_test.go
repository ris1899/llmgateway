package router

import (
	"testing"
	"time"

	"llmgateway/internal/config"
	"llmgateway/internal/llm"
)

func newTestDeployment(id string, priority int, cfg config.Breaker) *Deployment {
	return &Deployment{
		ID: id, Group: "g", Priority: priority,
		Stats:   NewStats(10 * time.Second),
		Breaker: NewBreaker(cfg, nil),
	}
}

func defaultBreakerCfg() config.Breaker {
	return config.Breaker{
		ConsecutiveFailures: 5,
		ErrorRate:           0.5,
		MinRequests:         5,
		Cooldown:            time.Minute,
		HalfOpenSuccesses:   2,
	}
}

func testGroup(strategy string, deployments ...*Deployment) *Group {
	return &Group{Name: "g", Strategy: strategy, Deployments: deployments}
}

func simpleRequest() *llm.ChatRequest {
	return &llm.ChatRequest{Messages: []llm.Message{{Role: llm.RoleUser, Content: "hi"}}}
}

func planIDs(t *testing.T, r *Router, g *Group, p Policy) []string {
	t.Helper()
	plan := r.Plan(g, p, simpleRequest())
	ids := make([]string, len(plan))
	for i, d := range plan {
		ids[i] = d.ID
	}
	return ids
}

func TestPlan_PriorityOrderPreserved(t *testing.T) {
	t.Parallel()
	cfg := defaultBreakerCfg()
	a := newTestDeployment("a", 1, cfg)
	b := newTestDeployment("b", 2, cfg)
	c := newTestDeployment("c", 3, cfg)
	g := testGroup(config.StrategyPriority, c, a, b) // deliberately out of order
	r := New(map[string]*Group{"g": g}, NewBudget(0.2, 10))

	got := planIDs(t, r, g, Policy{Strategy: config.StrategyPriority})
	want := []string{"a", "b", "c"}
	if !equalStrings(got, want) {
		t.Fatalf("plan order = %v, want %v", got, want)
	}
}

func TestPlan_OpenBreakerDeploymentsRemoved(t *testing.T) {
	t.Parallel()
	cfg := defaultBreakerCfg()
	a := newTestDeployment("a", 1, cfg)
	b := newTestDeployment("b", 2, cfg)
	g := testGroup(config.StrategyPriority, a, b)
	r := New(map[string]*Group{"g": g}, NewBudget(0.2, 10))

	now := time.Now()
	for i := 0; i < cfg.ConsecutiveFailures; i++ {
		a.Breaker.Report(now, false, Failure, i+1, i+1)
	}
	if a.Breaker.State() != Open {
		t.Fatal("setup: expected a's breaker to be open")
	}

	got := planIDs(t, r, g, Policy{Strategy: config.StrategyPriority})
	want := []string{"b"}
	if !equalStrings(got, want) {
		t.Fatalf("plan = %v, want %v (open-breaker deployment must be removed)", got, want)
	}
}

func TestPlan_FastestRanksByScoreAndPutsUnknownFirst(t *testing.T) {
	t.Parallel()
	cfg := defaultBreakerCfg()
	fast := newTestDeployment("fast", 1, cfg)
	slow := newTestDeployment("slow", 2, cfg)
	unknown := newTestDeployment("unknown", 3, cfg)

	for i := 0; i < minSamples; i++ {
		fast.Stats.ObserveNonStream(10*time.Millisecond, 0)
		slow.Stats.ObserveNonStream(time.Second, 0)
	}

	g := testGroup(config.StrategyFastest, fast, slow, unknown)
	r := New(map[string]*Group{"g": g}, NewBudget(0.2, 10))
	r.explore = 0 // deterministic: disable the random exploration slot swap

	got := planIDs(t, r, g, Policy{Strategy: config.StrategyFastest})
	want := []string{"unknown", "fast", "slow"}
	if !equalStrings(got, want) {
		t.Fatalf("plan = %v, want %v", got, want)
	}
}

func TestPlan_ErrorRateInflatesScore(t *testing.T) {
	t.Parallel()
	cfg := defaultBreakerCfg()
	healthy := newTestDeployment("healthy", 1, cfg)
	flaky := newTestDeployment("flaky", 2, cfg)

	now := time.Now()
	for i := 0; i < minSamples; i++ {
		healthy.Stats.ObserveNonStream(100*time.Millisecond, 0)
		flaky.Stats.ObserveNonStream(100*time.Millisecond, 0) // same predicted latency
	}
	for i := 0; i < 6; i++ { // flaky: 40% error rate, below trip threshold
		flaky.Stats.addResult(true, now)
	}
	for i := 0; i < 4; i++ {
		flaky.Stats.addResult(false, now)
	}

	g := testGroup(config.StrategyFastest, healthy, flaky)
	r := New(map[string]*Group{"g": g}, NewBudget(0.2, 10))
	r.explore = 0

	got := planIDs(t, r, g, Policy{Strategy: config.StrategyFastest})
	want := []string{"healthy", "flaky"}
	if !equalStrings(got, want) {
		t.Fatalf("plan = %v, want %v (higher error rate must rank worse)", got, want)
	}
}

func TestPlan_CheapestPicksCheapestMeetingLatencyTargetElseFastestFallback(t *testing.T) {
	t.Parallel()
	cfg := defaultBreakerCfg()
	cheapSlow := &Deployment{ID: "cheap-slow", Priority: 1, OutputPricePerM: 1,
		Stats: NewStats(10 * time.Second), Breaker: NewBreaker(cfg, nil)}
	pricyFast := &Deployment{ID: "pricy-fast", Priority: 2, OutputPricePerM: 100,
		Stats: NewStats(10 * time.Second), Breaker: NewBreaker(cfg, nil)}

	for i := 0; i < minSamples; i++ {
		cheapSlow.Stats.ObserveNonStream(time.Second, 0)         // misses a tight target
		pricyFast.Stats.ObserveNonStream(10*time.Millisecond, 0) // meets it
	}

	g := testGroup(config.StrategyCheapest, cheapSlow, pricyFast)
	r := New(map[string]*Group{"g": g}, NewBudget(0.2, 10))
	r.explore = 0

	got := planIDs(t, r, g, Policy{Strategy: config.StrategyCheapest, LatencyTarget: 100 * time.Millisecond})
	want := []string{"pricy-fast", "cheap-slow"}
	if !equalStrings(got, want) {
		t.Fatalf("plan with target = %v, want %v", got, want)
	}

	got = planIDs(t, r, g, Policy{Strategy: config.StrategyCheapest})
	want = []string{"cheap-slow", "pricy-fast"}
	if !equalStrings(got, want) {
		t.Fatalf("plan with no target = %v, want %v", got, want)
	}
}

func TestRankWeighted_DrawIsDeterministicGivenU(t *testing.T) {
	t.Parallel()
	cfg := defaultBreakerCfg()
	a := newTestDeployment("a", 1, cfg)
	b := newTestDeployment("b", 2, cfg)
	c := newTestDeployment("c", 3, cfg)
	a.Weight, b.Weight, c.Weight = 1, 1, 8 // weights sum to 10

	cands := []candidate{{d: a}, {d: b}, {d: c}}
	rankWeighted(cands, 0.05) // falls in a's [0, 0.1) slice
	if cands[0].d.ID != "a" {
		t.Fatalf("u=0.05: first = %s, want a", cands[0].d.ID)
	}

	cands = []candidate{{d: a}, {d: b}, {d: c}}
	rankWeighted(cands, 0.5) // falls in c's [0.2, 1.0) slice
	if cands[0].d.ID != "c" {
		t.Fatalf("u=0.5: first = %s, want c", cands[0].d.ID)
	}
}

func TestPlan_PreferMovesDeploymentToFrontButNotWhenBreakerOpen(t *testing.T) {
	t.Parallel()
	cfg := defaultBreakerCfg()
	a := newTestDeployment("a", 1, cfg)
	b := newTestDeployment("b", 2, cfg)
	c := newTestDeployment("c", 3, cfg)
	g := testGroup(config.StrategyPriority, a, b, c)
	r := New(map[string]*Group{"g": g}, NewBudget(0.2, 10))

	got := planIDs(t, r, g, Policy{Strategy: config.StrategyPriority, Prefer: "c"})
	want := []string{"c", "a", "b"}
	if !equalStrings(got, want) {
		t.Fatalf("plan with Prefer=c = %v, want %v", got, want)
	}

	now := time.Now() // trip c's breaker
	for i := 0; i < cfg.ConsecutiveFailures; i++ {
		c.Breaker.Report(now, false, Failure, i+1, i+1)
	}
	got = planIDs(t, r, g, Policy{Strategy: config.StrategyPriority, Prefer: "c"})
	want = []string{"a", "b"}
	if !equalStrings(got, want) {
		t.Fatalf("plan with Prefer=c after c's breaker opens = %v, want %v", got, want)
	}
}

func TestRouter_RecordResetsErrorsOnClose(t *testing.T) {
	t.Parallel()
	cfg := config.Breaker{
		ConsecutiveFailures: 10, // high, so only the error-rate path trips in this test
		ErrorRate:           0.5,
		MinRequests:         5,
		Cooldown:            0, // probe is allowed immediately after tripping
		HalfOpenSuccesses:   2,
	}
	d := newTestDeployment("d", 1, cfg)
	g := testGroup(config.StrategyPriority, d)
	r := New(map[string]*Group{"g": g}, NewBudget(1, 1000)) // budget not under test

	failErr := &llm.Error{Kind: llm.KindUnavailable}

	for i := 0; i < cfg.MinRequests; i++ {
		r.Record(d, false, failErr)
	}
	if d.Breaker.State() != Open {
		t.Fatal("setup: expected breaker to be open after the outage")
	}

	for i := 0; i < cfg.HalfOpenSuccesses; i++ {
		probe, ok := d.Breaker.Allow(time.Now())
		if !ok {
			t.Fatalf("probe %d: Allow = false, want true", i)
		}
		r.Record(d, probe, nil)
	}
	if d.Breaker.State() != Closed {
		t.Fatal("setup: expected breaker to be closed after successful probes")
	}

	r.Record(d, false, failErr) // error window was reset; must not re-trip
	if got := d.Breaker.State(); got != Closed {
		t.Fatalf("breaker state after one post-recovery failure = %v, want Closed (flapping bug)", got)
	}
}

func TestGroup_ExpectedOutputTreatsNonPositiveMaxTokensAsUnset(t *testing.T) {
	t.Parallel()
	g := &Group{}
	g.ObserveOutput(256) // establishes avgOut = 256 (first observation, no EWMA blend)

	for _, mt := range []int{0, -1, -100} {
		mt := mt
		req := &llm.ChatRequest{MaxTokens: &mt}
		if got := g.expectedOutput(req); got != 256 {
			t.Fatalf("expectedOutput with MaxTokens=%d = %d, want 256 (avg, cap ignored)", mt, got)
		}
	}

	cap := 50
	req := &llm.ChatRequest{MaxTokens: &cap}
	if got := g.expectedOutput(req); got != 50 {
		t.Fatalf("expectedOutput with MaxTokens=50 = %d, want 50", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
