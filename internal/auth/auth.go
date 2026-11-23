// Package auth validates API keys, cached in memory with Postgres as source of truth.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Prefix = "llmgw_"

const defaultQueryTimeout = 2 * time.Second

// dbBackoff: how long Lookup fails fast after a Postgres error.
const dbBackoff = 5 * time.Second

// maxCacheEntries bounds memory against a flood of invalid keys.
const maxCacheEntries = 100_000

type Key struct {
	ID              string
	Name            string
	RPS             float64
	Burst           int
	MonthlyCapUSD   float64
	DefaultStrategy string
}

var ErrInvalidKey = errors.New("invalid API key")

// ErrUnavailable is distinct from ErrInvalidKey: key may be valid, Postgres
// just can't confirm it. Callers answer 503, never 401.
var ErrUnavailable = errors.New("API key store unavailable")

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	db           querier
	ttl          time.Duration
	queryTimeout time.Duration

	mu    sync.RWMutex
	cache map[[32]byte]cached

	group     singleflight // collapses concurrent misses for the same key
	downUntil atomic.Int64 // unix nanos; misses fail fast until this passes
}

type cached struct {
	key     *Key // nil = known invalid
	expires time.Time
}

type Option func(*Store)

func WithQueryTimeout(d time.Duration) Option {
	return func(s *Store) { s.queryTimeout = d }
}

func NewStore(db *pgxpool.Pool, ttl time.Duration, opts ...Option) *Store {
	s := &Store{db: db, ttl: ttl, queryTimeout: defaultQueryTimeout, cache: map[[32]byte]cached{}}
	for _, o := range opts {
		o(s)
	}
	return s
}

func Hash(plain string) [32]byte { return sha256.Sum256([]byte(plain)) }

func (s *Store) Lookup(ctx context.Context, plain string) (*Key, error) {
	if !strings.HasPrefix(plain, Prefix) {
		return nil, ErrInvalidKey // cheap reject before any hashing or I/O
	}
	h := Hash(plain)
	now := time.Now()
	s.mu.RLock()
	c, ok := s.cache[h]
	s.mu.RUnlock()
	if ok && now.Before(c.expires) {
		if c.key == nil {
			return nil, ErrInvalidKey
		}
		return c.key, nil
	}

	if now.UnixNano() < s.downUntil.Load() {
		if ok && c.key != nil {
			return c.key, nil
		}
		return nil, ErrUnavailable
	}

	kp, err := s.group.Do(h, func() (*Key, error) { return s.query(ctx, h) })
	if err != nil {
		if ok && c.key != nil {
			return c.key, nil
		}
		return nil, err
	}
	if kp == nil {
		return nil, ErrInvalidKey
	}
	return kp, nil
}

func (s *Store) query(ctx context.Context, h [32]byte) (*Key, error) {
	// Detached from caller cancellation: this query serves every request
	// waiting on it via singleflight, not just the one that triggered it.
	qctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.queryTimeout)
	defer cancel()

	var k Key
	err := s.db.QueryRow(qctx, `SELECT id, name, rps, burst, monthly_cap_usd, default_strategy
		FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, h[:]).
		Scan(&k.ID, &k.Name, &k.RPS, &k.Burst, &k.MonthlyCapUSD, &k.DefaultStrategy)

	var kp *Key
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		s.downUntil.Store(time.Now().Add(dbBackoff).UnixNano())
		return nil, err
	default:
		kp = &k
	}

	s.mu.Lock()
	s.evict()
	s.cache[h] = cached{key: kp, expires: time.Now().Add(s.ttl)}
	s.mu.Unlock()
	return kp, nil
}

// evict drops expired, then negative, then oldest-expiring positive
// entries until under maxCacheEntries. Must be called with s.mu held.
func (s *Store) evict() {
	if len(s.cache) < maxCacheEntries {
		return
	}
	now := time.Now()
	for h, c := range s.cache {
		if now.After(c.expires) {
			delete(s.cache, h)
		}
	}
	if len(s.cache) < maxCacheEntries {
		return
	}
	for h, c := range s.cache {
		if c.key == nil {
			delete(s.cache, h)
		}
	}
	if len(s.cache) < maxCacheEntries {
		return
	}
	type kv struct {
		h [32]byte
		c cached
	}
	entries := make([]kv, 0, len(s.cache))
	for h, c := range s.cache {
		entries = append(entries, kv{h, c})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].c.expires.Before(entries[j].c.expires) })
	for _, e := range entries[:len(entries)/10+1] { // trim ~10%
		delete(s.cache, e.h)
	}
}

func Generate() (plain string, hash [32]byte) {
	var b [32]byte
	_, _ = rand.Read(b[:])
	plain = Prefix + base64.RawURLEncoding.EncodeToString(b[:])
	return plain, Hash(plain)
}

func NewID() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "key_" + base64.RawURLEncoding.EncodeToString(b[:])
}

type ctxKey struct{}

func WithKey(ctx context.Context, k *Key) context.Context { return context.WithValue(ctx, ctxKey{}, k) }

func FromContext(ctx context.Context) *Key {
	k, _ := ctx.Value(ctxKey{}).(*Key)
	return k
}
