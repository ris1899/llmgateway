package router

import (
	"sync"
	"time"

	"llmgateway/internal/config"
)

type State int32

const (
	Closed   State = iota // normal: traffic flows
	Open                  // tripped: no traffic until cooldown passes
	HalfOpen              // testing: one probe at a time decides recovery
)

func (s State) String() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half_open"
	default:
		return "closed"
	}
}

type Outcome int

const (
	Success Outcome = iota
	Failure
	Neutral // says nothing about provider health (bad request, client left)
)

// Breaker is a per-deployment circuit breaker.
//
// Why it exists: without it, every request to a dead provider pays the full
// failure cost (a timeout for a hung provider) before failing over. With it,
// after a few failures the provider is skipped entirely and requests go
// straight to a healthy one. That turns "every request waits 5s" into "a
// handful of requests wait 5s".
//
// Two trip conditions, because they catch different failures:
//   - N consecutive failures: hard-down provider; trips within N requests.
//   - error rate over the window: a provider failing 60% of calls never
//     produces 5 failures in a row reliably, but is still unusable.
//
// Recovery: after Cooldown, half-open lets ONE probe through at a time and
// needs several consecutive successes to close. One probe at a time means a
// still-dead provider costs one request per probe, not a burst.
//
// A mutex rather than atomics: the critical sections are a few instructions,
// contention is per deployment, and a lock-free state machine with probe
// accounting is much harder to get right.
type Breaker struct {
	mu       sync.Mutex
	cfg      config.Breaker
	state    State
	consec   int
	openedAt time.Time
	probing  bool
	probeOK  int
	onChange func(from, to State)
}

func NewBreaker(cfg config.Breaker, onChange func(from, to State)) *Breaker {
	return &Breaker{cfg: cfg, onChange: onChange}
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

// Peek reports whether Allow would admit a request now, without claiming the
// probe slot. The router uses it to build plans.
func (b *Breaker) Peek(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case Open:
		return now.Sub(b.openedAt) >= b.cfg.Cooldown
	case HalfOpen:
		return !b.probing
	default:
		return true
	}
}

// Allow is called right before an attempt. probe=true means this attempt is
// the half-open probe and its outcome must be reported with probe=true.
func (b *Breaker) Allow(now time.Time) (probe, ok bool) {
	b.mu.Lock()
	from := b.state
	switch b.state {
	case Closed:
		b.mu.Unlock()
		return false, true
	case Open:
		if now.Sub(b.openedAt) < b.cfg.Cooldown {
			b.mu.Unlock()
			return false, false
		}
		b.state, b.probeOK, b.probing = HalfOpen, 0, false
	}
	if b.probing {
		b.mu.Unlock()
		b.notify(from, HalfOpen)
		return false, false
	}
	b.probing = true
	b.mu.Unlock()
	b.notify(from, HalfOpen)
	return true, true
}

// Report feeds an attempt's outcome. n and fails are the deployment's
// request and failure counts over the stats window.
func (b *Breaker) Report(now time.Time, probe bool, o Outcome, n, fails int) {
	b.mu.Lock()
	from := b.state
	if probe {
		b.probing = false
	}
	switch b.state {
	case Closed:
		switch o {
		case Success:
			b.consec = 0
		case Failure:
			b.consec++
			rateTrip := n >= b.cfg.MinRequests && float64(fails)/float64(n) >= b.cfg.ErrorRate
			if b.consec >= b.cfg.ConsecutiveFailures || rateTrip {
				b.trip(now)
			}
		}
	case HalfOpen:
		// Non-probe results arriving now were started before the trip; they
		// don't tell us whether the provider recovered, so ignore them.
		if probe {
			switch o {
			case Success:
				b.probeOK++
				if b.probeOK >= b.cfg.HalfOpenSuccesses {
					b.state, b.consec = Closed, 0
				}
			case Failure:
				b.trip(now)
			}
		}
	case Open:
		// Late results from requests started before the trip: ignore.
	}
	to := b.state
	b.mu.Unlock()
	b.notify(from, to)
}

func (b *Breaker) trip(now time.Time) {
	b.state, b.openedAt, b.consec, b.probing = Open, now, 0, false
}

// notify runs outside the lock so callbacks (metrics, logs) can't deadlock or
// slow down other requests.
func (b *Breaker) notify(from, to State) {
	if from != to && b.onChange != nil {
		b.onChange(from, to)
	}
}
