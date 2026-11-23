package ratelimit

import (
	"testing"
	"time"
)

func TestToNanoUSD(t *testing.T) {
	t.Parallel()
	cases := []struct {
		usd  float64
		want int64
	}{
		{0, 0},
		{1, 1_000_000_000},
		{0.000000001, 1},         // smallest representable unit
		{1.23456789, 1234567890}, // rounds, not truncates
		{-1, -1_000_000_000},
	}
	for _, c := range cases {
		if got := toNanoUSD(c.usd); got != c.want {
			t.Errorf("toNanoUSD(%v) = %d, want %d", c.usd, got, c.want)
		}
	}
}

func TestSpendKeyIsPerKeyPerUTCMonth(t *testing.T) {
	t.Parallel()
	jan := time.Date(2026, time.January, 15, 12, 0, 0, 0, time.UTC)
	febSameInstantDifferentZone := time.Date(2026, time.February, 1, 0, 30, 0, 0, time.FixedZone("x", 5*3600)) // 2026-01-31T19:30 UTC

	if got, want := spendKey("key_a", jan), "llmgw:spend:key_a:202601"; got != want {
		t.Errorf("spendKey = %q, want %q", got, want)
	}
	if spendKey("key_a", jan) == spendKey("key_b", jan) {
		t.Error("spendKey must be distinct per key id")
	}
	// Same moment in two time zones must land in the same bucket: the key is
	// derived from the UTC calendar month, not the zone the time.Time carries.
	janUTCEquivalent := febSameInstantDifferentZone.UTC()
	if janUTCEquivalent.Month() != time.January {
		t.Fatalf("test setup: expected the fixture to land in January UTC, got %v", janUTCEquivalent)
	}
	if got, want := spendKey("key_a", febSameInstantDifferentZone), "llmgw:spend:key_a:202601"; got != want {
		t.Errorf("spendKey across zones = %q, want %q", got, want)
	}
}

func TestSpendKeyRollsOverAtMonthBoundary(t *testing.T) {
	t.Parallel()
	lastOfJan := time.Date(2026, time.January, 31, 23, 59, 59, 0, time.UTC)
	firstOfFeb := lastOfJan.Add(2 * time.Second)
	if spendKey("key_a", lastOfJan) == spendKey("key_a", firstOfFeb) {
		t.Error("spendKey should differ once the UTC month rolls over")
	}
}
