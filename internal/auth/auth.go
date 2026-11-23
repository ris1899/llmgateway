// Package auth validates API keys.
//
// Keys look like "llmgw_<random>". Only SHA-256(key) is stored. A plain fast
// hash is correct here (unlike passwords, which need bcrypt/argon2): API keys
// are 32 random bytes, so brute-forcing the hash is infeasible anyway, and a
// slow hash would add tens of ms to every request.
//
// Lookups are cached in memory for CacheTTL. That keeps Postgres off the hot
// path (one query per key per TTL, not per request) at the cost of a
// revocation delay of up to CacheTTL. Unknown keys are cached too (negative
// caching) so a client hammering with a bad key can't hammer Postgres.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Prefix = "llmgw_"

type Key struct {
	ID              string
	Name            string
	RPS             float64
	Burst           int
	MonthlyCapUSD   float64
	DefaultStrategy string
}

var ErrInvalidKey = errors.New("invalid API key")

type Store struct {
	db  *pgxpool.Pool
	ttl time.Duration

	mu    sync.RWMutex
	cache map[[32]byte]cached
}

type cached struct {
	key     *Key // nil = known invalid
	expires time.Time
}

func NewStore(db *pgxpool.Pool, ttl time.Duration) *Store {
	return &Store{db: db, ttl: ttl, cache: map[[32]byte]cached{}}
}

func Hash(plain string) [32]byte { return sha256.Sum256([]byte(plain)) }

// Lookup resolves a plaintext key.
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

	var k Key
	err := s.db.QueryRow(ctx, `SELECT id, name, rps, burst, monthly_cap_usd, default_strategy
		FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL`, h[:]).
		Scan(&k.ID, &k.Name, &k.RPS, &k.Burst, &k.MonthlyCapUSD, &k.DefaultStrategy)
	var kp *Key
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		// DB down: if we have a stale entry, keep using it (availability over
		// freshness); otherwise the request fails with 503, not 401, because
		// the key may well be valid.
		if ok && c.key != nil {
			return c.key, nil
		}
		return nil, err
	default:
		kp = &k
	}
	s.mu.Lock()
	if len(s.cache) > 100_000 { // crude bound against memory abuse via random keys
		clear(s.cache)
	}
	s.cache[h] = cached{key: kp, expires: now.Add(s.ttl)}
	s.mu.Unlock()
	if kp == nil {
		return nil, ErrInvalidKey
	}
	return kp, nil
}

// Generate creates a new random key. Returns plaintext (show once) and hash.
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

// FromContext returns the authenticated key, or nil when auth is disabled.
func FromContext(ctx context.Context) *Key {
	k, _ := ctx.Value(ctxKey{}).(*Key)
	return k
}
