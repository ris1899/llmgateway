package router

import (
	"sync"
	"time"

	"llmgateway/internal/config"
)

type State int32

const (
	Closed State = iota
	Open
	HalfOpen
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
	Neutral // bad request, client left: says nothing about provider health
)

// Breaker is a per-deployment circuit breaker: skips a dead provider instead
// of paying its full timeout per request; recovers via single probes.
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

// Peek checks admission without claiming the probe slot.
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

// Allow reports admission; probe=true means report its outcome with probe=true too.
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

// Report feeds an outcome and returns (from, to) state for transition handling.
func (b *Breaker) Report(now time.Time, probe bool, o Outcome, n, fails int) (from, to State) {
	b.mu.Lock()
	from = b.state
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
		if probe { // non-probe results predate the trip; ignore
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
	case Open: // late result predating the trip: ignore
	}
	to = b.state
	b.mu.Unlock()
	b.notify(from, to)
	return from, to
}

func (b *Breaker) trip(now time.Time) {
	b.state, b.openedAt, b.consec, b.probing = Open, now, 0, false
}

// runs outside the lock so callbacks can't deadlock
func (b *Breaker) notify(from, to State) {
	if from != to && b.onChange != nil {
		b.onChange(from, to)
	}
}
