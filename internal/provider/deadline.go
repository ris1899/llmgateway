package provider

import (
	"context"
	"sync/atomic"
	"time"
)

// Deadline is a per-attempt "no progress" timeout.
//
// A plain context.WithTimeout doesn't fit streaming: a 60s cap would kill a
// healthy 2-minute stream. Instead the timer covers time-to-first-byte and is
// then reset on every received event, so it means "upstream went silent for
// d", which catches hung providers without capping long answers.
//
// It also remembers whether *it* fired, so errors can be classified correctly:
// our timeout is the provider's fault (Timeout), while the parent context being
// canceled means the client left (Canceled) and must not hurt provider health.
type Deadline struct {
	Ctx    context.Context
	parent context.Context
	cancel context.CancelFunc
	d      time.Duration
	timer  *time.Timer
	fired  atomic.Bool
}

func NewDeadline(parent context.Context, d time.Duration) *Deadline {
	ctx, cancel := context.WithCancel(parent)
	dl := &Deadline{Ctx: ctx, parent: parent, cancel: cancel, d: d}
	dl.timer = time.AfterFunc(d, func() {
		dl.fired.Store(true)
		cancel()
	})
	return dl
}

// Extend restarts the timer after progress (a received event).
func (dl *Deadline) Extend() {
	if !dl.fired.Load() {
		dl.timer.Reset(dl.d)
	}
}

// Release stops the timer and cancels the attempt context. Always call it.
func (dl *Deadline) Release() {
	dl.timer.Stop()
	dl.cancel()
}
