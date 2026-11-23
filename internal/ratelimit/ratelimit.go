// Package ratelimit enforces per-API-key request rate (token bucket) and
// monthly spend caps, both in Redis so limits hold across gateway replicas.
//
// Spend cap semantics (soft cap): the check happens before the request, but
// the cost is only known after it. Requests already in flight when a key
// crosses its cap still complete, so a key can overshoot by roughly
// (concurrent requests x cost per request). A hard cap would need to reserve
// the worst-case cost (max_tokens x price) up front and refund the
// difference, which rejects legitimate requests near the limit. Soft cap is
// the usual trade-off for LLM billing.
package ratelimit

import (
	"context"
	_ "embed"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed limiter.lua
var luaSrc string

// redis.NewScript sends EVALSHA (just the script hash) and falls back to EVAL
// only if Redis doesn't have the script cached, so the script body isn't
// shipped on every request.
var script = redis.NewScript(luaSrc)

type Limits struct {
	RPS           float64
	Burst         int
	MonthlyCapUSD float64
}

type Decision int

const (
	Allowed Decision = iota
	RateLimited
	OverSpendCap
)

type Limiter struct {
	rdb       *redis.Client
	opTimeout time.Duration
}

func New(rdb *redis.Client, opTimeout time.Duration) *Limiter {
	return &Limiter{rdb: rdb, opTimeout: opTimeout}
}

// Monthly key per UTC calendar month. Old months expire on their own.
func spendKey(keyID string, now time.Time) string {
	return fmt.Sprintf("llmgw:spend:%s:%s", keyID, now.UTC().Format("200601"))
}

func toNanoUSD(usd float64) int64 { return int64(math.Round(usd * 1e9)) }

// Check returns the decision and, when rate limited, how long to wait.
// On Redis errors the caller decides (we fail open, see DECISIONS.md).
func (l *Limiter) Check(ctx context.Context, keyID string, lim Limits) (Decision, time.Duration, error) {
	if lim.RPS <= 0 && lim.MonthlyCapUSD <= 0 {
		return Allowed, 0, nil
	}
	rps, burst := lim.RPS, lim.Burst
	if rps <= 0 { // no rate limit, only a spend cap: make the bucket effectively infinite
		rps, burst = 1e9, 1e9
	}
	if burst < 1 {
		burst = max(1, int(math.Ceil(rps)))
	}
	ctx, cancel := context.WithTimeout(ctx, l.opTimeout)
	defer cancel()
	res, err := script.Run(ctx, l.rdb,
		[]string{"llmgw:rl:" + keyID, spendKey(keyID, time.Now())},
		rps, burst, toNanoUSD(lim.MonthlyCapUSD)).Int64Slice()
	if err != nil {
		return Allowed, 0, err
	}
	switch res[0] {
	case 1:
		return Allowed, 0, nil
	case -1:
		return OverSpendCap, 0, nil
	default:
		return RateLimited, time.Duration(res[1]) * time.Millisecond, nil
	}
}

// AddSpend records a completed request's cost. INCRBY on integer nano-USD
// instead of INCRBYFLOAT: float accumulation drifts over millions of tiny
// additions; integers don't.
func (l *Limiter) AddSpend(ctx context.Context, keyID string, usd float64) error {
	n := toNanoUSD(usd)
	if n <= 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.opTimeout)
	defer cancel()
	k := spendKey(keyID, time.Now())
	pipe := l.rdb.Pipeline()
	pipe.IncrBy(ctx, k, n)
	pipe.Expire(ctx, k, 35*24*time.Hour)
	_, err := pipe.Exec(ctx)
	return err
}

// Spent returns this month's spend for a key (used by keyctl).
func (l *Limiter) Spent(ctx context.Context, keyID string) (float64, error) {
	n, err := l.rdb.Get(ctx, spendKey(keyID, time.Now())).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return float64(n) / 1e9, err
}
