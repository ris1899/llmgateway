package router

import (
	"testing"
	"time"
)

func TestStats_PredictUnknownBelowMinSamples(t *testing.T) {
	t.Parallel()
	s := NewStats(time.Second)
	now := time.Unix(0, 0)

	for i := 0; i < minSamples-1; i++ {
		s.ObserveNonStream(100*time.Millisecond, 10)
	}
	if _, known := s.Snapshot(now).Predict(10); known {
		t.Fatal("Predict known = true with fewer than minSamples observations")
	}

	s.ObserveNonStream(100*time.Millisecond, 10)
	if _, known := s.Snapshot(now.Add(time.Hour)).Predict(10); !known {
		t.Fatal("Predict known = false at minSamples observations")
	}
}

func TestStats_PercentilesP50P95(t *testing.T) {
	t.Parallel()
	s := NewStats(time.Second)
	for i := 1; i <= 10; i++ { // 100ms..1000ms, tpot=0 so duration==total
		s.ObserveNonStream(time.Duration(i*100)*time.Millisecond, 0)
	}
	snap := s.Snapshot(time.Unix(0, 0).Add(time.Hour)) // force cache refresh
	if snap.BaseP50 != 600*time.Millisecond {
		t.Fatalf("BaseP50 = %v, want 600ms", snap.BaseP50)
	}
	if snap.BaseP95 != 1000*time.Millisecond {
		t.Fatalf("BaseP95 = %v, want 1000ms", snap.BaseP95)
	}
}

func TestStats_ObserveTPOTUsesEWMA(t *testing.T) {
	t.Parallel()
	s := NewStats(time.Second)
	s.ObserveTPOT(900*time.Millisecond, 10) // 100ms/token, sets initial value
	if s.tpot != 100*time.Millisecond {
		t.Fatalf("tpot after first observation = %v, want 100ms", s.tpot)
	}
	s.ObserveTPOT(1800*time.Millisecond, 10) // 200ms/token
	want := (100*time.Millisecond*9 + 200*time.Millisecond) / 10
	if s.tpot != want {
		t.Fatalf("tpot after EWMA update = %v, want %v", s.tpot, want)
	}
}

func TestStats_ObserveTPOTIgnoresSingleTokenGenerations(t *testing.T) {
	t.Parallel()
	s := NewStats(time.Second)
	s.ObserveTPOT(time.Second, 1) // tokens-1 == 0, can't derive a per-token rate
	if s.tpot != 0 {
		t.Fatalf("tpot = %v, want 0 (single-token generation must be ignored)", s.tpot)
	}
}

func TestStats_ErrorWindowBucketsRollOverAfterWindowPasses(t *testing.T) {
	t.Parallel()
	window := time.Second // bucketDur = 100ms
	s := NewStats(window)
	now := time.Unix(0, 0)

	s.addResult(true, now)
	s.addResult(false, now)
	if n, fails := s.counts(now); n != 2 || fails != 1 {
		t.Fatalf("counts right after = (%d,%d), want (2,1)", n, fails)
	}

	within := now.Add(window / 2)
	if n, fails := s.counts(within); n != 2 || fails != 1 {
		t.Fatalf("counts within window = (%d,%d), want (2,1)", n, fails)
	}

	after := now.Add(window + 10*time.Millisecond)
	if n, fails := s.counts(after); n != 0 || fails != 0 {
		t.Fatalf("counts after window rolled over = (%d,%d), want (0,0)", n, fails)
	}
}

func TestStats_ErrorRateComputedFromWindowCounts(t *testing.T) {
	t.Parallel()
	s := NewStats(time.Second)
	now := time.Unix(0, 0)

	for i := 0; i < 3; i++ {
		s.addResult(true, now)
	}
	for i := 0; i < 1; i++ {
		s.addResult(false, now)
	}
	snap := s.Snapshot(now)
	if snap.Requests != 4 || snap.Failures != 1 {
		t.Fatalf("Requests,Failures = %d,%d, want 4,1", snap.Requests, snap.Failures)
	}
	if snap.ErrorRate != 0.25 {
		t.Fatalf("ErrorRate = %v, want 0.25", snap.ErrorRate)
	}
}

func TestStats_ResetErrorsClearsWindowButKeepsLatency(t *testing.T) {
	t.Parallel()
	s := NewStats(time.Second)
	now := time.Unix(0, 0)

	s.addResult(false, now)
	s.addResult(false, now)
	for i := 0; i < minSamples; i++ {
		s.ObserveNonStream(100*time.Millisecond, 0)
	}

	s.resetErrors()

	if n, fails := s.counts(now); n != 0 || fails != 0 {
		t.Fatalf("counts after resetErrors = (%d,%d), want (0,0)", n, fails)
	}
	if _, known := s.Snapshot(now.Add(time.Hour)).Predict(10); !known {
		t.Fatal("resetErrors must not clear latency samples")
	}
}
