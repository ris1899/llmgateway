package provider

import (
	"context"
	"testing"
	"time"
)

func waitForFired(t *testing.T, dl *Deadline, want bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if dl.fired.Load() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("fired = %v after waiting, want %v", dl.fired.Load(), want)
}

func TestDeadline_FiresOnNoProgress(t *testing.T) {
	t.Parallel()
	dl := NewDeadline(context.Background(), 20*time.Millisecond)
	defer dl.Release()

	waitForFired(t, dl, true)
	if dl.Ctx.Err() == nil {
		t.Fatal("Ctx should be canceled once the deadline fires")
	}
}

func TestDeadline_ExtendPostponesFiring(t *testing.T) {
	t.Parallel()
	dl := NewDeadline(context.Background(), 30*time.Millisecond)
	defer dl.Release()

	stop := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(stop) {
		dl.Extend()
		time.Sleep(5 * time.Millisecond)
	}
	if dl.fired.Load() {
		t.Fatal("deadline fired despite continuous progress")
	}

	waitForFired(t, dl, true)
}

func TestDeadline_ReleaseStopsTimer(t *testing.T) {
	t.Parallel()
	dl := NewDeadline(context.Background(), 20*time.Millisecond)
	dl.Release()

	time.Sleep(40 * time.Millisecond)
	if dl.fired.Load() {
		t.Fatal("fired should remain false after Release stopped the timer in time")
	}
	if dl.Ctx.Err() == nil {
		t.Fatal("Release must still cancel Ctx so in-flight requests unblock")
	}
}

func TestDeadline_ParentCancelPropagates(t *testing.T) {
	t.Parallel()
	parent, cancel := context.WithCancel(context.Background())
	dl := NewDeadline(parent, time.Second)
	defer dl.Release()

	cancel()
	<-dl.Ctx.Done()
	if dl.fired.Load() {
		t.Fatal("parent cancellation must not be reported as the deadline firing")
	}
}
