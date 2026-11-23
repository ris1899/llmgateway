// Package cache is an exact-match response cache in Redis.
//
// What is cached: complete, successful responses to requests that are
// effectively deterministic (explicit low temperature). The key is a hash of
// everything that changes the answer: model group, messages and sampling
// params. The deployment that served it is NOT in the key: the client asked
// for a model group, and any member's answer is a valid answer.
//
// Failure policy: every Redis error is treated as a miss (fail open). The cache
// is an optimization; it must never be the reason a request fails or slows
// down, so every call has a short timeout.
package cache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"llmgateway/internal/llm"
)

type Cache struct {
	rdb       *redis.Client
	ttl       time.Duration
	maxTemp   float64
	opTimeout time.Duration
	// downUntil (unix nanos): after a Redis error, skip the cache entirely for
	// a few seconds. Without this, a Redis outage adds opTimeout (x2: get+set)
	// to every request; with it, only the first few requests pay.
	downUntil atomic.Int64
}

const backoff = 5 * time.Second

// ErrUnavailable is returned while the cache is in backoff after an error.
var ErrUnavailable = errors.New("cache unavailable (backing off)")

func New(rdb *redis.Client, ttl time.Duration, maxTemp float64, opTimeout time.Duration) *Cache {
	return &Cache{rdb: rdb, ttl: ttl, maxTemp: maxTemp, opTimeout: opTimeout}
}

func (c *Cache) available() bool { return time.Now().UnixNano() >= c.downUntil.Load() }

func (c *Cache) markDown(err error) error {
	if err != nil && !errors.Is(err, context.Canceled) {
		c.downUntil.Store(time.Now().Add(backoff).UnixNano())
	}
	return err
}

// keyMaterial is the canonical form we hash. A struct (not the raw request
// body) means field order, whitespace and irrelevant fields (stream,
// stream_options, user) don't produce different keys for the same question.
type keyMaterial struct {
	V           int           `json:"v"` // bump to invalidate everything on format change
	Group       string        `json:"g"`
	Messages    []llm.Message `json:"m"`
	Temperature *float64      `json:"t"`
	TopP        *float64      `json:"p"`
	MaxTokens   *int          `json:"x"`
	Stop        []string      `json:"s"`
}

// Key returns the cache key, or ok=false if the request must not be cached.
func (c *Cache) Key(req *llm.ChatRequest) (key string, ok bool) {
	if req.Temperature == nil || *req.Temperature > c.maxTemp {
		return "", false
	}
	b, err := json.Marshal(keyMaterial{V: 1, Group: req.Model, Messages: req.Messages,
		Temperature: req.Temperature, TopP: req.TopP, MaxTokens: req.MaxTokens, Stop: req.Stop})
	if err != nil {
		return "", false
	}
	sum := sha256.Sum256(b)
	return "llmgw:cache:" + hex.EncodeToString(sum[:]), true
}

// Entry is what's stored: the response plus who originally served it, so a
// hit can still report the provider and usage of the original call.
type Entry struct {
	Response   llm.ChatResponse `json:"r"`
	Deployment string           `json:"d"`
}

// Get returns (entry, true, nil) on hit, (nil, false, nil) on miss, and
// (nil, false, err) on Redis failure, which callers treat as a miss.
func (c *Cache) Get(ctx context.Context, key string) (*Entry, bool, error) {
	if !c.available() {
		return nil, false, ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, c.opTimeout)
	defer cancel()
	b, err := c.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, c.markDown(err)
	}
	var e Entry
	if err := json.Unmarshal(b, &e); err != nil {
		return nil, false, err
	}
	return &e, true, nil
}

func (c *Cache) Set(ctx context.Context, key string, e *Entry) error {
	if !c.available() {
		return ErrUnavailable
	}
	b, err := json.Marshal(e)
	if err != nil {
		return err
	}
	// Detached from the request context: the client may already be gone, but
	// the answer is still worth keeping.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opTimeout)
	defer cancel()
	return c.markDown(c.rdb.Set(ctx, key, b, c.ttl).Err())
}
