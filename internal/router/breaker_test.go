package router

import (
	"sync"
	"testing"
	"time"

	"llmgateway/internal/config"
)

func testBreakerCfg() config.Breaker {
	return config.Breaker{
		ConsecutiveFailures: 3,
		ErrorRate:           0.5,
		MinRequests:         4,
		Cooldown:            time.Second,
		HalfOpenSuccesses:   2,
	}
}

func TestBreaker_ClosedTripsOnConsecutiveFailures(t *testing.T) {
	t.Parallel()
	b := NewBreaker(testBreakerCfg(), nil)
	now := time.Unix(0, 0)

	b.Report(now, false, Failure, 2, 2)
	b.Report(now, false, Failure, 2, 2)
	if got := b.State(); got != Closed {
		t.Fatalf("state after 2 failures = %v, want Closed", got)
	}

	b.Report(now, false, Failure, 3, 3)
	if got := b.State(); got != Open {
		t.Fatalf("state after 3 consecutive failures = %v, want Open", got)
	}
}

func TestBreaker_ClosedTripsOnErrorRateOnlyOnceMinRequestsReached(t *testing.T) {
	t.Parallel()
	b := NewBreaker(testBreakerCfg(), nil)
	now := time.Unix(0, 0)

	b.Report(now, false, Failure, 2, 1) // 50% rate but below MinRequests
	if got := b.State(); got != Closed {
		t.Fatalf("state below min_requests = %v, want Closed", got)
	}

	b.Report(now, false, Failure, 4, 2)
	if got := b.State(); got != Open {
		t.Fatalf("state at min_requests with error rate met = %v, want Open", got)
	}
}

func TestBreaker_OpenStaysOpenBeforeCooldown(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)
	trip3x(b, now)

	if _, ok := b.Allow(now.Add(cfg.Cooldown / 2)); ok {
		t.Fatal("Allow before cooldown elapsed = true, want false")
	}
	if b.Peek(now.Add(cfg.Cooldown / 2)) {
		t.Fatal("Peek before cooldown elapsed = true, want false")
	}
}

func TestBreaker_OpenTransitionsToHalfOpenAfterCooldown(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)
	trip3x(b, now)

	probeTime := now.Add(cfg.Cooldown)
	if !b.Peek(probeTime) {
		t.Fatal("Peek after cooldown elapsed = false, want true")
	}
	probe, ok := b.Allow(probeTime)
	if !probe || !ok {
		t.Fatalf("Allow after cooldown = (%v, %v), want (true, true)", probe, ok)
	}
	if got := b.State(); got != HalfOpen {
		t.Fatalf("state after first post-cooldown Allow = %v, want HalfOpen", got)
	}
}

func TestBreaker_HalfOpenAllowsSingleProbe(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)
	trip3x(b, now)

	probeTime := now.Add(cfg.Cooldown)
	const n = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	probes := 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			probe, ok := b.Allow(probeTime)
			if probe && ok {
				mu.Lock()
				probes++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if probes != 1 {
		t.Fatalf("concurrent probes claimed = %d, want 1", probes)
	}
}

func TestBreaker_HalfOpenClosesAfterConfiguredSuccesses(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)
	trip3x(b, now)

	probeTime := now.Add(cfg.Cooldown)
	for i := 0; i < cfg.HalfOpenSuccesses; i++ {
		probe, ok := b.Allow(probeTime)
		if !probe || !ok {
			t.Fatalf("probe %d: Allow = (%v, %v), want (true, true)", i, probe, ok)
		}
		b.Report(probeTime, true, Success, 1, 0)
		wantState := HalfOpen
		if i == cfg.HalfOpenSuccesses-1 {
			wantState = Closed
		}
		if got := b.State(); got != wantState {
			t.Fatalf("probe %d: state = %v, want %v", i, got, wantState)
		}
	}
}

func TestBreaker_ProbeFailureReopens(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)
	trip3x(b, now)

	probeTime := now.Add(cfg.Cooldown)
	probe, ok := b.Allow(probeTime)
	if !probe || !ok {
		t.Fatalf("Allow = (%v, %v), want (true, true)", probe, ok)
	}
	b.Report(probeTime, true, Failure, 1, 1)
	if got := b.State(); got != Open {
		t.Fatalf("state after failed probe = %v, want Open", got)
	}

	if _, ok := b.Allow(probeTime.Add(time.Millisecond)); ok { // re-tripped; needs cooldown again
		t.Fatal("Allow immediately after failed probe = true, want false")
	}
}

func TestBreaker_NonProbeOutcomesIgnoredInHalfOpen(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)
	trip3x(b, now)

	probeTime := now.Add(cfg.Cooldown)
	if _, ok := b.Allow(probeTime); !ok {
		t.Fatal("expected probe to be allowed")
	}
	b.Report(probeTime, false, Failure, 5, 5)
	b.Report(probeTime, false, Success, 5, 0)
	if got := b.State(); got != HalfOpen {
		t.Fatalf("state after non-probe reports = %v, want HalfOpen", got)
	}
}

func TestBreaker_NeutralOutcomeNeverTrips(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)

	for i := 0; i < 10; i++ {
		b.Report(now, false, Neutral, 0, 0)
	}
	if got := b.State(); got != Closed {
		t.Fatalf("state after repeated Neutral outcomes = %v, want Closed", got)
	}
}

func TestBreaker_ReportReturnsTransition(t *testing.T) {
	t.Parallel()
	cfg := testBreakerCfg()
	b := NewBreaker(cfg, nil)
	now := time.Unix(0, 0)

	for i := 0; i < cfg.ConsecutiveFailures-1; i++ {
		if from, to := b.Report(now, false, Failure, i+1, i+1); from != Closed || to != Closed {
			t.Fatalf("report %d: (from,to) = (%v,%v), want (Closed,Closed)", i, from, to)
		}
	}
	from, to := b.Report(now, false, Failure, cfg.ConsecutiveFailures, cfg.ConsecutiveFailures)
	if from != Closed || to != Open {
		t.Fatalf("tripping report: (from,to) = (%v,%v), want (Closed,Open)", from, to)
	}
}

func trip3x(b *Breaker, now time.Time) {
	cfg := b.cfg
	for i := 0; i < cfg.ConsecutiveFailures; i++ {
		b.Report(now, false, Failure, i+1, i+1)
	}
}
