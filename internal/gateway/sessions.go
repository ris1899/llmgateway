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

// SessionStore maps a conversation to the deployment that served its last
// turn, so follow-up turns keep hitting that provider's prompt cache.
type SessionStore interface {
	Get(ctx context.Context, key string) (deploymentID string, ok bool)
	Set(ctx context.Context, key, deploymentID string)
}

// scoped by API key + group so tenants can't collide; hashed to a fixed-size key
func sessionKey(scoped, group string) string {
	sum := sha256.Sum256([]byte(scoped + "\x00" + group))
	return "llmgw:session:" + hex.EncodeToString(sum[:])
}

// MemorySessions works for a single instance; use Redis behind a load balancer.
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

// RedisSessions fails open: on a Redis error, requests just aren't sticky.
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

// Set refreshes the TTL so an active conversation never expires.
func (s *RedisSessions) Set(ctx context.Context, key, id string) {
	if s.down() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.opTimeout)
	defer cancel()
	s.markDown(s.rdb.Set(ctx, key, id, s.ttl).Err())
}

func (g *Gateway) stickyLookup(ctx context.Context, grpName string, opt Options) (key, prefer string) {
	if g.sessions == nil || opt.Session == "" {
		return "", ""
	}
	key = sessionKey(opt.Session, grpName)
	prefer, _ = g.sessions.Get(ctx, key)
	return key, prefer
}

// labels the outcome: new, hit (same deployment), or moved (failover)
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
