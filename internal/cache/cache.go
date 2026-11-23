// Package cache is an exact-match response cache in Redis for deterministic (low-temperature) requests.
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
	downUntil atomic.Int64 // unix nanos; skip cache until this passes, after a Redis error
}

const backoff = 5 * time.Second

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

// keyMaterial is the canonical form hashed for the cache key.
type keyMaterial struct {
	V           int           `json:"v"` // bump to invalidate on format change
	Group       string        `json:"g"`
	Messages    []llm.Message `json:"m"`
	Temperature *float64      `json:"t"`
	TopP        *float64      `json:"p"`
	MaxTokens   *int          `json:"x"`
	Stop        []string      `json:"s"`
}

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

type Entry struct {
	Response   llm.ChatResponse `json:"r"`
	Deployment string           `json:"d"`
}

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
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opTimeout) // detached: worth keeping even if client left
	defer cancel()
	return c.markDown(c.rdb.Set(ctx, key, b, c.ttl).Err())
}
