package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// SessionStore remembers which deployment served a conversation last, so the
// next turn of the same conversation goes to the same provider (sticky
// routing). Why it matters for LLMs specifically:
//   - Providers cache the start of a long prompt (prompt caching). The next
//     turn re-sends the whole history, and on the same provider most of it is
//     a cache hit: cheaper input tokens and lower time to first token.
//     Switching providers throws that cache away.
//   - A conversation that switches models mid-way changes tone and style.
//
// The mapping stores the deployment that served the LAST turn, not the first:
// after a failover, the provider-side prompt cache lives on the new
// deployment, so that is where the conversation should stay.
type SessionStore interface {
	Get(ctx context.Context, key string) (deploymentID string, ok bool)
	Set(ctx context.Context, key, deploymentID string)
}

// sessionKey scopes a session id by API key and model group: one tenant can't
// steer another tenant's conversations by guessing session ids, and deployment
// ids are only meaningful within a group. Hashed so client-supplied ids of any
// shape become a fixed-size key.
func sessionKey(scoped, group string) string {
	sum := sha256.Sum256([]byte(scoped + "\x00" + group))
	return "llmgw:session:" + hex.EncodeToString(sum[:])
}

// --- in-memory store: used when Redis isn't configured ---

// MemorySessions works for a single gateway instance. With several replicas
// behind a load balancer, each would learn its own mapping, so use Redis.
type MemorySessions struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string]memEntry
}

type memEntry struct {
	id  string
	exp time.Time
}

const maxMemSessions = 100_000 // bound memory if clients send endless new ids

func NewMemorySessions(ttl time.Duration) *MemorySessions {
	return &MemorySessions{ttl: ttl, m: map[string]memEntry{}}
}

func (s *MemorySessions) Get(_ context.Context, key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok || time.Now().After(e.exp) {
		return "", false
	}
	return e.id, true
}

func (s *MemorySessions) Set(_ context.Context, key, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if len(s.m) >= maxMemSessions {
		for k, e := range s.m {
			if now.After(e.exp) {
				delete(s.m, k)
			}
		}
		if len(s.m) >= maxMemSessions {
			clear(s.m) // worst case: conversations lose stickiness once, nothing breaks
		}
	}
	s.m[key] = memEntry{id: id, exp: now.Add(s.ttl)}
}

// --- Redis store: shared by all gateway replicas ---

// RedisSessions fails open like the cache: on a Redis error, requests simply
// aren't sticky for a few seconds, and only the first one pays the timeout.
type RedisSessions struct {
	rdb       *redis.Client
	ttl       time.Duration
	opTimeout time.Duration
	downUntil atomic.Int64
}

func NewRedisSessions(rdb *redis.Client, ttl, opTimeout time.Duration) *RedisSessions {
	return &RedisSessions{rdb: rdb, ttl: ttl, opTimeout: opTimeout}
}

func (s *RedisSessions) down() bool { return time.Now().UnixNano() < s.downUntil.Load() }

func (s *RedisSessions) markDown(err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		s.downUntil.Store(time.Now().Add(5 * time.Second).UnixNano())
	}
}

func (s *RedisSessions) Get(ctx context.Context, key string) (string, bool) {
	if s.down() {
		return "", false
	}
	ctx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	v, err := s.rdb.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false
	}
	if err != nil {
		s.markDown(err)
		return "", false
	}
	return v, true
}

// Set also refreshes the TTL, so an active conversation never expires; an
// idle one is forgotten after ttl (by then the provider's prompt cache has
// expired too, so stickiness no longer buys anything).
func (s *RedisSessions) Set(ctx context.Context, key, id string) {
	if s.down() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opTimeout)
	defer cancel()
	s.markDown(s.rdb.Set(ctx, key, id, s.ttl).Err())
}

// --- used by Chat / ChatStream ---

// stickyLookup returns the session key and the deployment that served this
// conversation last ("" if none, or if the request has no session).
func (g *Gateway) stickyLookup(ctx context.Context, grpName string, opt Options) (key, prefer string) {
	if g.sessions == nil || opt.Session == "" {
		return "", ""
	}
	key = sessionKey(opt.Session, grpName)
	prefer, _ = g.sessions.Get(ctx, key)
	return key, prefer
}

// stickyUpdate records who served this turn and labels the outcome:
// new (first turn), hit (same deployment as last turn), moved (failover or
// the previous deployment's breaker was open).
func (g *Gateway) stickyUpdate(ctx context.Context, key, prefer, served string) string {
	if key == "" {
		return ""
	}
	g.sessions.Set(ctx, key, served)
	result := "moved"
	switch prefer {
	case "":
		result = "new"
	case served:
		result = "hit"
	}
	g.m.Sticky.WithLabelValues(result).Inc()
	return result
}
