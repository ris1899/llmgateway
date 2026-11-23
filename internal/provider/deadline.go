package provider

import (
	"context"
	"sync/atomic"
	"time"
)

// Deadline is a per-attempt "no progress" timeout, reset on each received
// event rather than capping the whole stream.
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

func (dl *Deadline) Extend() {
	if !dl.fired.Load() {
		dl.timer.Reset(dl.d)
	}
}

func (dl *Deadline) Release() {
	dl.timer.Stop()
	dl.cancel()
}
