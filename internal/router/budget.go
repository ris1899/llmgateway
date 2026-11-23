package router

import (
	"sync"
	"time"
)

const budgetWindowSecs = 10

// Budget caps paid retries/hedges to a fraction of traffic over a sliding
// 10s window, so a degradation can't become a retry storm. Failover away
// from an already-open breaker is free (not budgeted).
type Budget struct {
	mu        sync.Mutex
	ratio     float64
	minPerSec float64
	buckets   [budgetWindowSecs]budgetBucket
}

type budgetBucket struct {
	sec           int64
	reqs, retries int
}

func NewBudget(ratio, minPerSec float64) *Budget {
	return &Budget{ratio: ratio, minPerSec: minPerSec}
}

func (b *Budget) cur(now time.Time) *budgetBucket {
	sec := now.Unix()
	bk := &b.buckets[sec%budgetWindowSecs]
	if bk.sec != sec {
		*bk = budgetBucket{sec: sec}
	}
	return bk
}

func (b *Budget) OnRequest(now time.Time) {
	b.mu.Lock()
	b.cur(now).reqs++
	b.mu.Unlock()
}

func (b *Budget) TryRetry(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	cur := b.cur(now)
	var reqs, retries int
	for _, bk := range b.buckets {
		if bk.sec > now.Unix()-budgetWindowSecs {
			reqs += bk.reqs
			retries += bk.retries
		}
	}
	if float64(retries) < b.ratio*float64(reqs)+b.minPerSec*budgetWindowSecs {
		cur.retries++
		return true
	}
	return false
}
