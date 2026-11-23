// Package ratelimit enforces per-key rate and monthly spend caps in Redis across gateway replicas.
package ratelimit

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

//go:embed limiter.lua
var luaSrc string

var script = redis.NewScript(luaSrc) // EVALSHA, falls back to EVAL on cache miss

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

// Monthly key, UTC calendar month. Limitation: Check and AddSpend run
// minutes apart, so a request straddling a month boundary is checked
// against one month and billed to the next.
func spendKey(keyID string, now time.Time) string {
	return fmt.Sprintf("llmgw:spend:%s:%s", keyID, now.UTC().Format("200601"))
}

func toNanoUSD(usd float64) int64 { return int64(math.Round(usd * 1e9)) }

// Check leaves fail-open-on-Redis-error decisions to the caller.
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

// AddSpend uses INCRBY on integer nano-USD rather than INCRBYFLOAT to avoid float drift.
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

func (l *Limiter) Spent(ctx context.Context, keyID string) (float64, error) {
	n, err := l.rdb.Get(ctx, spendKey(keyID, time.Now())).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	return float64(n) / 1e9, err
}
