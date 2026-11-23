package gateway

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"llmgateway/internal/config"
	"llmgateway/internal/llm"
	"llmgateway/internal/metrics"
	"llmgateway/internal/provider"
	"llmgateway/internal/router"
)

const testGroup = "g"

func generousBreaker() config.Breaker {
	return config.Breaker{ConsecutiveFailures: 100, ErrorRate: 1, MinRequests: 1_000_000, Cooldown: time.Minute, HalfOpenSuccesses: 1}
}

func deployment(id string, p provider.Provider, breaker config.Breaker) *router.Deployment {
	return &router.Deployment{
		ID: id, Group: testGroup, Provider: p, Model: "m",
		Stats: router.NewStats(time.Second), Breaker: router.NewBreaker(breaker, nil),
	}
}

func tripOpen(d *router.Deployment, cfg config.Breaker) {
	for i := 0; i < cfg.ConsecutiveFailures; i++ {
		d.Breaker.Report(time.Now(), false, router.Failure, i+1, i+1)
	}
}

func newTestGateway(budgetRatio, budgetMinPerSec float64, hedgeAfter time.Duration, deployments ...*router.Deployment) *Gateway {
	g := &router.Group{Name: testGroup, Strategy: config.StrategyPriority, HedgeAfter: hedgeAfter, Deployments: deployments}
	r := router.New(map[string]*router.Group{testGroup: g}, router.NewBudget(budgetRatio, budgetMinPerSec))
	log := slog.New(slog.DiscardHandler)
	return New(r, nil, nil, metrics.New(), log)
}

func chatReq() *llm.ChatRequest {
	return &llm.ChatRequest{Model: testGroup, Messages: []llm.Message{{Role: llm.RoleUser, Content: "hi"}}}
}

func asFailure(t *testing.T, err error) *Failure {
	t.Helper()
	var f *Failure
	if !errors.As(err, &f) {
		t.Fatalf("error %v (%T) is not a *gateway.Failure", err, err)
	}
	return f
}

func TestChatSuccess(t *testing.T) {
	t.Parallel()
	d := deployment("p/a", newFakeProvider("p"), generousBreaker())
	g := newTestGateway(1, 1000, 0, d)

	res, err := g.Chat(context.Background(), chatReq(), Options{})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if res.Attempts != 1 || res.DeployID != "p/a" || res.Hedged {
		t.Errorf("got Attempts=%d DeployID=%q Hedged=%v, want 1, p/a, false", res.Attempts, res.DeployID, res.Hedged)
	}
}

func TestChatFailoverOnRetryableError(t *testing.T) {
	t.Parallel()
	bad := deployment("p/bad", newFakeProvider("p", fakeCall{err: retryableErr(llm.KindUnavailable)}), generousBreaker())
	good := deployment("p/good", newFakeProvider("p", fakeCall{}), generousBreaker())
	g := newTestGateway(1, 1000, 0, bad, good)

	res, err := g.Chat(context.Background(), chatReq(), Options{})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if res.Attempts != 2 || res.DeployID != "p/good" {
		t.Errorf("got Attempts=%d DeployID=%q, want 2, p/good", res.Attempts, res.DeployID)
	}
}

func TestChatNoFailoverOnBadRequest(t *testing.T) {
	t.Parallel()
	bad := deployment("p/bad", newFakeProvider("p", fakeCall{err: retryableErr(llm.KindBadRequest)}), generousBreaker())
	good := deployment("p/good", newFakeProvider("p", fakeCall{}), generousBreaker())
	g := newTestGateway(1, 1000, 0, bad, good)

	_, err := g.Chat(context.Background(), chatReq(), Options{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	f := asFailure(t, err)
	if f.Attempts != 1 || f.DeployID != "p/bad" {
		t.Errorf("got Attempts=%d DeployID=%q, want 1, p/bad (no failover on bad request)", f.Attempts, f.DeployID)
	}
	if good.Provider.(*fakeProvider).calls.Load() != 0 {
		t.Error("the healthy deployment should never have been called")
	}
}

func TestChatOpenBreakerDeploymentSkipped(t *testing.T) {
	t.Parallel()
	cfg := generousBreaker()
	cfg.ConsecutiveFailures = 1
	down := deployment("p/down", newFakeProvider("p"), cfg)
	tripOpen(down, cfg)
	up := deployment("p/up", newFakeProvider("p", fakeCall{}), generousBreaker())
	g := newTestGateway(0, 0, 0, down, up) // 0 budget: failover must still be free

	res, err := g.Chat(context.Background(), chatReq(), Options{})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if res.Attempts != 1 || res.DeployID != "p/up" {
		t.Errorf("got Attempts=%d DeployID=%q, want 1, p/up", res.Attempts, res.DeployID)
	}
	if down.Provider.(*fakeProvider).calls.Load() != 0 {
		t.Error("the open-breaker deployment should never have been called")
	}
}

func TestChatAllBreakersOpenFailsFast(t *testing.T) {
	t.Parallel()
	cfg := generousBreaker()
	cfg.ConsecutiveFailures = 1
	a := deployment("p/a", newFakeProvider("p"), cfg)
	b := deployment("p/b", newFakeProvider("p"), cfg)
	tripOpen(a, cfg)
	tripOpen(b, cfg)
	g := newTestGateway(1, 1000, 0, a, b)

	_, err := g.Chat(context.Background(), chatReq(), Options{})
	if !errors.Is(err, ErrNoHealthy) {
		t.Fatalf("err = %v, want ErrNoHealthy", err)
	}
	var f *Failure
	if errors.As(err, &f) {
		t.Errorf("ErrNoHealthy before any attempt should not carry a Failure, got %+v", f)
	}
}

func TestChatRetryBudgetExhaustedStopsFailover(t *testing.T) {
	t.Parallel()
	first := deployment("p/first", newFakeProvider("p", fakeCall{err: retryableErr(llm.KindUnavailable)}), generousBreaker())
	second := deployment("p/second", newFakeProvider("p", fakeCall{}), generousBreaker())
	g := newTestGateway(0, 0, 0, first, second)

	_, err := g.Chat(context.Background(), chatReq(), Options{})
	f := asFailure(t, err)
	if f.Attempts != 1 || f.DeployID != "p/first" {
		t.Errorf("got Attempts=%d DeployID=%q, want 1, p/first (budget exhausted)", f.Attempts, f.DeployID)
	}
	if second.Provider.(*fakeProvider).calls.Load() != 0 {
		t.Error("second deployment should never have been called: no retry budget")
	}
}

func TestChatHedging(t *testing.T) {
	t.Parallel()
	slowCall, release := blockingCall()
	slow := deployment("p/slow", newFakeProvider("p", slowCall), generousBreaker())
	fast := deployment("p/fast", newFakeProvider("p", fakeCall{}), generousBreaker())
	g := newTestGateway(1, 1000, 5*time.Millisecond, slow, fast)
	defer close(release)

	res, err := g.Chat(context.Background(), chatReq(), Options{})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if !res.Hedged || res.DeployID != "p/fast" {
		t.Errorf("got Hedged=%v DeployID=%q, want true, p/fast", res.Hedged, res.DeployID)
	}
	if res.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", res.Attempts)
	}
}

func TestChatFailureCarriesAttemptMetadata(t *testing.T) {
	t.Parallel()
	a := deployment("p/a", newFakeProvider("p", fakeCall{err: retryableErr(llm.KindUnavailable)}), generousBreaker())
	b := deployment("p/b", newFakeProvider("p", fakeCall{err: retryableErr(llm.KindUnavailable)}), generousBreaker())
	g := newTestGateway(1, 1000, 0, a, b)

	_, err := g.Chat(context.Background(), chatReq(), Options{})
	f := asFailure(t, err)
	if f.Attempts != 2 {
		t.Errorf("Attempts = %d, want 2", f.Attempts)
	}
	if f.DeployID != "p/b" {
		t.Errorf("DeployID = %q, want p/b (last deployment tried)", f.DeployID)
	}
}

func TestChatStreamFailoverBeforeFirstChunk(t *testing.T) {
	t.Parallel()
	bad := deployment("p/bad", newFakeProvider("p", fakeCall{err: retryableErr(llm.KindUnavailable)}), generousBreaker())
	good := deployment("p/good", newFakeProvider("p", fakeCall{chunks: []llm.StreamChunk{{Delta: "hi"}}}), generousBreaker())
	g := newTestGateway(1, 1000, 0, bad, good)

	st, err := g.ChatStream(context.Background(), chatReq(), Options{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	defer st.Close()
	if st.Attempts != 2 || st.Deployment.ID != "p/good" {
		t.Errorf("got Attempts=%d Deployment=%q, want 2, p/good", st.Attempts, st.Deployment.ID)
	}
}

func TestChatStreamMidStreamErrorNotFailedOver(t *testing.T) {
	t.Parallel()
	p := newFakeProvider("p", fakeCall{
		chunks:  []llm.StreamChunk{{Delta: "hi"}},
		failAt:  1,
		failErr: retryableErr(llm.KindUnavailable),
	})
	only := deployment("p/only", p, generousBreaker())
	g := newTestGateway(1, 1000, 0, only)

	st, err := g.ChatStream(context.Background(), chatReq(), Options{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	defer st.Close()

	if _, err := st.Recv(); err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	_, err = st.Recv()
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want the mid-stream error to surface", err)
	}
	if p.calls.Load() != 1 {
		t.Errorf("provider called %d times, want 1: a mid-stream error must not fail over", p.calls.Load())
	}
}

func TestChatStreamFailureCarriesAttemptMetadata(t *testing.T) {
	t.Parallel()
	a := deployment("p/a", newFakeProvider("p", fakeCall{err: retryableErr(llm.KindUnavailable)}), generousBreaker())
	g := newTestGateway(1, 1000, 0, a)

	_, err := g.ChatStream(context.Background(), chatReq(), Options{})
	f := asFailure(t, err)
	if f.Attempts != 1 || f.DeployID != "p/a" {
		t.Errorf("got Attempts=%d DeployID=%q, want 1, p/a", f.Attempts, f.DeployID)
	}
}

func TestStickySessions(t *testing.T) {
	t.Parallel()
	cfg := generousBreaker()
	cfg.ConsecutiveFailures = 1
	a := deployment("p/a", newFakeProvider("p", fakeCall{}), cfg)
	b := deployment("p/b", newFakeProvider("p", fakeCall{}), cfg)
	g := newTestGateway(1, 1000, 0, a, b)
	g.sessions = NewMemorySessions(time.Minute)

	opt := Options{Session: "conv-1"}
	res1, err := g.Chat(context.Background(), chatReq(), opt)
	if err != nil {
		t.Fatalf("turn 1: %v", err)
	}
	if res1.Sticky != "new" {
		t.Errorf("turn 1 Sticky = %q, want new", res1.Sticky)
	}

	res2, err := g.Chat(context.Background(), chatReq(), opt)
	if err != nil {
		t.Fatalf("turn 2: %v", err)
	}
	if res2.Sticky != "hit" || res2.DeployID != res1.DeployID {
		t.Errorf("turn 2 Sticky=%q DeployID=%q, want hit, %q", res2.Sticky, res2.DeployID, res1.DeployID)
	}

	var prevDep *router.Deployment
	if res1.DeployID == "p/a" {
		prevDep = a
	} else {
		prevDep = b
	}
	tripOpen(prevDep, cfg)

	res3, err := g.Chat(context.Background(), chatReq(), opt)
	if err != nil {
		t.Fatalf("turn 3: %v", err)
	}
	if res3.Sticky != "moved" || res3.DeployID == res1.DeployID {
		t.Errorf("turn 3 Sticky=%q DeployID=%q, want moved, away from %q", res3.Sticky, res3.DeployID, res1.DeployID)
	}
}
