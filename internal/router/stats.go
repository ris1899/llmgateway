package router

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

const (
	latencySamples = 256
	minSamples     = 5 // below this, latency is "unknown"
	errBuckets     = 10
	snapshotTTL    = 100 * time.Millisecond // percentile recompute throttle
)

// Stats tracks live health for one deployment. Latency is split into base
// (pre-generation) and TPOT (time/output token), so long answers aren't
// mistaken for slow ones.
type Stats struct {
	mu        sync.Mutex
	bucketDur time.Duration
	buckets   [errBuckets]errBucket

	lat     [latencySamples]time.Duration
	scratch [latencySamples]time.Duration
	latN    int
	latPos  int
	tpot    time.Duration // EWMA

	snap   Snapshot // cached percentiles
	snapAt time.Time

	inflight atomic.Int64
}

type errBucket struct {
	epoch    int64
	ok, fail int
}

type Snapshot struct {
	BaseP50, BaseP95 time.Duration
	TPOT             time.Duration
	Samples          int
	Requests         int // in the error window
	Failures         int
	ErrorRate        float64
	InFlight         int64
}

func (s Snapshot) Predict(expectedOut int) (d time.Duration, known bool) {
	if s.Samples < minSamples {
		return 0, false
	}
	return s.BaseP95 + time.Duration(expectedOut)*s.TPOT, true
}

func NewStats(window time.Duration) *Stats {
	return &Stats{bucketDur: max(window/errBuckets, time.Millisecond)}
}

// lazily resets a reused slot; no background goroutine needed
func (s *Stats) bucket(now time.Time) *errBucket {
	epoch := now.UnixNano() / int64(s.bucketDur)
	b := &s.buckets[epoch%errBuckets]
	if b.epoch != epoch {
		*b = errBucket{epoch: epoch}
	}
	return b
}

func (s *Stats) addResult(ok bool, now time.Time) {
	s.mu.Lock()
	b := s.bucket(now)
	if ok {
		b.ok++
	} else {
		b.fail++
	}
	s.mu.Unlock()
}

func (s *Stats) countsLocked(now time.Time) (n, fails int) {
	cur := now.UnixNano() / int64(s.bucketDur)
	for _, b := range s.buckets {
		if b.epoch > cur-errBuckets && b.epoch <= cur {
			n += b.ok + b.fail
			fails += b.fail
		}
	}
	return n, fails
}

func (s *Stats) counts(now time.Time) (n, fails int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.countsLocked(now)
}

func (s *Stats) resetErrors() {
	s.mu.Lock()
	s.buckets = [errBuckets]errBucket{}
	s.mu.Unlock()
}

func (s *Stats) pushLocked(d time.Duration) {
	s.lat[s.latPos] = d
	s.latPos = (s.latPos + 1) % latencySamples
	if s.latN < latencySamples {
		s.latN++
	}
}

func (s *Stats) ObserveTTFT(d time.Duration) {
	s.mu.Lock()
	s.pushLocked(d)
	s.mu.Unlock()
}

func (s *Stats) ObserveTPOT(generation time.Duration, tokens int) {
	if tokens < 2 {
		return
	}
	d := generation / time.Duration(tokens-1)
	s.mu.Lock()
	if s.tpot == 0 {
		s.tpot = d
	} else {
		s.tpot = (s.tpot*9 + d) / 10 // EWMA, alpha 0.1
	}
	s.mu.Unlock()
}

func (s *Stats) ObserveNonStream(total time.Duration, outTokens int) {
	s.mu.Lock()
	s.pushLocked(max(0, total-time.Duration(outTokens)*s.tpot))
	s.mu.Unlock()
}

func (s *Stats) Snapshot(now time.Time) Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Sub(s.snapAt) >= snapshotTTL {
		v := s.scratch[:s.latN]
		copy(v, s.lat[:s.latN])
		slices.Sort(v)
		s.snap = Snapshot{Samples: s.latN, TPOT: s.tpot}
		if n := len(v); n > 0 {
			s.snap.BaseP50 = v[n*50/100]
			s.snap.BaseP95 = v[min(n-1, n*95/100)]
		}
		s.snapAt = now
	}
	snap := s.snap
	snap.Requests, snap.Failures = s.countsLocked(now)
	if snap.Requests > 0 {
		snap.ErrorRate = float64(snap.Failures) / float64(snap.Requests)
	}
	snap.InFlight = s.inflight.Load()
	return snap
}

func (s *Stats) IncInFlight() { s.inflight.Add(1) }
func (s *Stats) DecInFlight() { s.inflight.Add(-1) }
