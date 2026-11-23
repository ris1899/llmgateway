package router

import (
	"testing"
	"time"
)

func TestBudget_RetriesCappedAtRatioOfTraffic(t *testing.T) {
	t.Parallel()
	b := NewBudget(0.2, 0) // 100 reqs * 0.2 ratio = 20 retries
	now := time.Unix(1000, 0)

	for i := 0; i < 100; i++ {
		b.OnRequest(now)
	}

	allowed := 0
	for i := 0; i < 30; i++ {
		if b.TryRetry(now) {
			allowed++
		}
	}
	if allowed != 20 {
		t.Fatalf("retries allowed = %d, want 20", allowed)
	}
}

func TestBudget_MinPerSecFloorAtZeroTraffic(t *testing.T) {
	t.Parallel()
	b := NewBudget(0.2, 1) // 1/sec floor * 10s window = 10 retries
	now := time.Unix(1000, 0)

	allowed := 0
	for i := 0; i < 15; i++ {
		if b.TryRetry(now) {
			allowed++
		}
	}
	if allowed != 10 {
		t.Fatalf("retries allowed at zero traffic = %d, want 10", allowed)
	}
}

func TestBudget_RefillsAsWindowSlidesForward(t *testing.T) {
	t.Parallel()
	b := NewBudget(0.2, 0)
	now := time.Unix(1000, 0)

	for i := 0; i < 10; i++ {
		b.OnRequest(now)
	}
	first, second := b.TryRetry(now), b.TryRetry(now)
	if !first || !second {
		t.Fatal("expected first two retries to be allowed")
	}
	if b.TryRetry(now) {
		t.Fatal("expected budget to be exhausted")
	}

	later := now.Add(budgetWindowSecs * time.Second)
	if b.TryRetry(later) {
		t.Fatal("expected no budget left after window rolled over with no new traffic")
	}

	for i := 0; i < 10; i++ {
		b.OnRequest(later)
	}
	if !b.TryRetry(later) {
		t.Fatal("expected a retry to be allowed after new traffic funded the window")
	}
}
