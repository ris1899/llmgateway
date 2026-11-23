package auth

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

type fakeRow struct {
	key *Key
	err error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*string) = r.key.ID
	*dest[1].(*string) = r.key.Name
	*dest[2].(*float64) = r.key.RPS
	*dest[3].(*int) = r.key.Burst
	*dest[4].(*float64) = r.key.MonthlyCapUSD
	*dest[5].(*string) = r.key.DefaultStrategy
	return nil
}

type fakeQuerier struct {
	mu      sync.Mutex
	queries int
	delay   time.Duration // simulates a slow/hung query
	result  func(call int) (pgx.Row, error)
}

func (f *fakeQuerier) QueryRow(ctx context.Context, _ string, _ ...any) pgx.Row {
	f.mu.Lock()
	f.queries++
	n := f.queries
	f.mu.Unlock()
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return fakeRow{err: ctx.Err()}
		}
	}
	row, err := f.result(n)
	if err != nil {
		return fakeRow{err: err}
	}
	return row
}

func newStoreWithQuerier(q querier, ttl time.Duration, opts ...Option) *Store {
	s := NewStore(nil, ttl, opts...)
	s.db = q
	return s
}

func constResult(k *Key, err error) func(int) (pgx.Row, error) {
	return func(int) (pgx.Row, error) {
		if err != nil {
			return nil, err
		}
		return fakeRow{key: k}, nil
	}
}

func TestLookupRejectsWrongPrefix(t *testing.T) {
	t.Parallel()
	s := newStoreWithQuerier(&fakeQuerier{result: constResult(nil, errors.New("should not be called"))}, time.Minute)
	if _, err := s.Lookup(context.Background(), "nope"); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("got %v, want ErrInvalidKey", err)
	}
}

func TestLookupHitCachesAndStopsQuerying(t *testing.T) {
	t.Parallel()
	want := &Key{ID: "key_1", Name: "demo", RPS: 10, Burst: 20, MonthlyCapUSD: 5}
	q := &fakeQuerier{result: constResult(want, nil)}
	s := newStoreWithQuerier(q, time.Minute)

	for i := 0; i < 5; i++ {
		got, err := s.Lookup(context.Background(), Prefix+"abc")
		if err != nil {
			t.Fatalf("Lookup: %v", err)
		}
		if got.ID != want.ID {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
	if q.queries != 1 {
		t.Fatalf("queries = %d, want 1 (rest should be served from cache)", q.queries)
	}
}

func TestLookupMissIsNegativelyCached(t *testing.T) {
	t.Parallel()
	q := &fakeQuerier{result: constResult(nil, pgx.ErrNoRows)}
	s := newStoreWithQuerier(q, time.Minute)

	for i := 0; i < 3; i++ {
		if _, err := s.Lookup(context.Background(), Prefix+"bad"); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("got %v, want ErrInvalidKey", err)
		}
	}
	if q.queries != 1 {
		t.Fatalf("queries = %d, want 1 (negative result should be cached too)", q.queries)
	}
}

func TestLookupExpiresAfterTTL(t *testing.T) {
	t.Parallel()
	want := &Key{ID: "key_1"}
	q := &fakeQuerier{result: constResult(want, nil)}
	s := newStoreWithQuerier(q, 10*time.Millisecond)

	if _, err := s.Lookup(context.Background(), Prefix+"abc"); err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := s.Lookup(context.Background(), Prefix+"abc"); err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if q.queries != 2 {
		t.Fatalf("queries = %d, want 2 (entry should have expired)", q.queries)
	}
}

func TestLookupServesStaleOnDBError(t *testing.T) {
	t.Parallel()
	want := &Key{ID: "key_1"}
	var fail atomic.Bool
	q := &fakeQuerier{result: func(int) (pgx.Row, error) {
		if fail.Load() {
			return nil, errors.New("db down")
		}
		return fakeRow{key: want}, nil
	}}
	s := newStoreWithQuerier(q, 10*time.Millisecond)

	if _, err := s.Lookup(context.Background(), Prefix+"abc"); err != nil {
		t.Fatalf("warm the cache: %v", err)
	}
	time.Sleep(20 * time.Millisecond) // let the entry go stale
	fail.Store(true)

	got, err := s.Lookup(context.Background(), Prefix+"abc")
	if err != nil {
		t.Fatalf("Lookup during DB outage: %v", err)
	}
	if got.ID != want.ID {
		t.Fatalf("got %+v, want stale %+v", got, want)
	}
}

func TestLookupFailsClosedOnDBErrorWithoutStaleEntry(t *testing.T) {
	t.Parallel()
	q := &fakeQuerier{result: constResult(nil, errors.New("db down"))}
	s := newStoreWithQuerier(q, time.Minute)

	if _, err := s.Lookup(context.Background(), Prefix+"abc"); err == nil || errors.Is(err, ErrInvalidKey) {
		t.Fatalf("got %v, want the underlying DB error, not ErrInvalidKey", err)
	}
}

func TestLookupBackoffFailsFastAfterDBError(t *testing.T) {
	t.Parallel()
	q := &fakeQuerier{result: constResult(nil, errors.New("db down"))}
	s := newStoreWithQuerier(q, time.Minute)

	if _, err := s.Lookup(context.Background(), Prefix+"first"); err == nil {
		t.Fatal("want error on first (uncached) lookup")
	}
	_, err := s.Lookup(context.Background(), Prefix+"second")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err during backoff = %v, want ErrUnavailable", err)
	}
	if errors.Is(err, ErrInvalidKey) {
		t.Fatal("backoff must not report a possibly-valid key as invalid")
	}
	if q.queries != 1 {
		t.Fatalf("queries = %d, want 1 (second lookup should fail fast during backoff)", q.queries)
	}
}

func TestLookupLeaderCancelDoesNotFailOthers(t *testing.T) {
	t.Parallel()
	want := &Key{ID: "key_1"}
	q := &fakeQuerier{delay: 100 * time.Millisecond, result: constResult(want, nil)}
	s := newStoreWithQuerier(q, time.Minute)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	go func() { _, _ = s.Lookup(leaderCtx, Prefix+"shared") }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		q.mu.Lock()
		started := q.queries == 1
		q.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("leader query never started")
		}
		time.Sleep(time.Millisecond)
	}

	type res struct {
		k   *Key
		err error
	}
	follower := make(chan res, 1)
	go func() {
		k, err := s.Lookup(context.Background(), Prefix+"shared")
		follower <- res{k, err}
	}()
	cancelLeader()

	got := <-follower
	if got.err != nil || got.k == nil || got.k.ID != want.ID {
		t.Fatalf("follower got (%v, %v), want key %q", got.k, got.err, want.ID)
	}
	if s.downUntil.Load() != 0 {
		t.Fatal("a client disconnect started the database backoff")
	}
}

func TestLookupSingleflightCollapsesConcurrentMisses(t *testing.T) {
	t.Parallel()
	want := &Key{ID: "key_1"}
	q := &fakeQuerier{delay: 30 * time.Millisecond, result: constResult(want, nil)}
	s := newStoreWithQuerier(q, time.Minute)

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Lookup(context.Background(), Prefix+"concurrent")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if q.queries != 1 {
		t.Fatalf("queries = %d, want 1 (concurrent misses should collapse into one)", q.queries)
	}
}

func TestEvictionKeepsPositiveEntriesOverNegativeFlood(t *testing.T) {
	t.Parallel()
	q := &fakeQuerier{result: func(n int) (pgx.Row, error) { return nil, pgx.ErrNoRows }}
	s := newStoreWithQuerier(q, time.Hour)

	realHash := Hash(Prefix + "real")
	s.mu.Lock()
	s.cache[realHash] = cached{key: &Key{ID: "key_real"}, expires: time.Now().Add(time.Hour)}
	s.mu.Unlock()

	for i := 0; i < maxCacheEntries+10; i++ {
		plain := Prefix + "flood" + string(rune(i))
		_, _ = s.Lookup(context.Background(), plain)
	}

	s.mu.RLock()
	c, ok := s.cache[realHash]
	s.mu.RUnlock()
	if !ok || c.key == nil || c.key.ID != "key_real" {
		t.Fatal("eviction destroyed the positive entry while the cache was flooded with negative ones")
	}
}
