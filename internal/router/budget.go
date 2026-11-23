package router

import (
	"sync"
	"time"
)

const budgetWindowSecs = 10

// Budget caps extra attempts (failovers into a provider that isn't known-dead,
// and hedges) to a fraction of real traffic over a sliding 10s window.
//
// Why: retries multiply load exactly when providers are struggling. If every
// request fails on A, retries on B, fails, retries on C, a partial degradation
// turns into 3x load on everything, which is a retry storm. With a 20% budget,
// retries can add at most ~20% load, no matter how bad things get.
//
// Failover away from a deployment whose breaker is already OPEN is free (not
// budgeted): the breaker already caps load on the dead provider, and those
// requests would have gone to the healthy one anyway. This is what lets the
// gateway fail over every in-flight request when one provider dies, without
// letting a broad degradation become a storm. Same model as Finagle/Envoy
// retry budgets: deposits from requests, withdrawals from retries.
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
