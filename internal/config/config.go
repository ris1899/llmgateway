// Package config loads and validates the gateway's YAML config.
//
// Design rules:
//   - Secrets never live in the file. Fields ending in _env name an env var
//     that we resolve at load time, so the YAML is safe to commit.
//   - Unknown fields are an error. A typo like "timout" silently falling back to
//     a default is the kind of bug you find in production at 3am.
//   - Validate everything up front and report all problems at once, so the
//     process either starts with a coherent config or refuses to start.
//   - Every infrastructure dependency is optional. No redis.addr = no cache and
//     no rate limits; no usage.brokers = no usage events. That keeps `make run`
//     dependency-free and lets benchmarks isolate one component at a time.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    Server              `yaml:"server"`
	Router    Router              `yaml:"router"`
	Redis     Redis               `yaml:"redis"`
	Cache     Cache               `yaml:"cache"`
	Auth      Auth                `yaml:"auth"`
	Usage     Usage               `yaml:"usage"`
	Providers map[string]Provider `yaml:"providers"`
	// Models maps a public model name (what clients send in "model") to the
	// deployments that can serve it. A name with several deployments is a
	// "model group": interchangeable models the router can choose between.
	Models map[string]Model `yaml:"models"`
}

type Server struct {
	Addr              string        `yaml:"addr"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	// No WriteTimeout field on purpose: net/http's WriteTimeout is a hard cap on
	// the whole response, which would kill long SSE streams. Per-request
	// deadlines are enforced in the handler instead.
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	MaxBodyBytes    int64         `yaml:"max_body_bytes"`
	// RequestTimeout bounds a whole non-streaming request across all failover
	// attempts, so a chain of slow providers can't hold a client forever.
	RequestTimeout time.Duration `yaml:"request_timeout"`
	// MetricsAddr serves /metrics, /admin/deployments and pprof on a separate
	// port so they are never exposed through the public load balancer.
	MetricsAddr string `yaml:"metrics_addr"`
}

type Router struct {
	Breaker     Breaker     `yaml:"breaker"`
	RetryBudget RetryBudget `yaml:"retry_budget"`
	// StatsWindow is how far back error rates look. Short = reacts fast but
	// noisy; long = stable but slow to notice recovery.
	StatsWindow time.Duration `yaml:"stats_window"`
	// SessionTTL: how long an idle conversation stays sticky to its
	// deployment. Provider prompt caches expire after minutes of idleness, so
	// remembering much longer buys nothing.
	SessionTTL time.Duration `yaml:"session_ttl"`
}

type Breaker struct {
	// Trip on N consecutive failures: catches a hard-down provider within a
	// handful of requests, regardless of traffic volume.
	ConsecutiveFailures int `yaml:"consecutive_failures"`
	// ...or on an error rate over the stats window: catches a provider that is
	// failing a large share of requests but not every single one.
	ErrorRate   float64 `yaml:"error_rate"`
	MinRequests int     `yaml:"min_requests"` // don't judge error rate on tiny samples
	// Cooldown before letting probe requests test an open breaker.
	Cooldown time.Duration `yaml:"cooldown"`
	// HalfOpenSuccesses consecutive probe successes are needed to close again,
	// so one lucky request doesn't flip a flapping provider back on.
	HalfOpenSuccesses int `yaml:"half_open_successes"`
}

type RetryBudget struct {
	// Ratio of extra attempts (failovers + hedges) allowed per original
	// request. 0.2 means retries can add at most ~20% load.
	Ratio float64 `yaml:"ratio"`
	// MinPerSec lets low-traffic gateways still retry.
	MinPerSec float64 `yaml:"min_per_sec"`
}

type Redis struct {
	Addr        string `yaml:"addr"` // empty = Redis features disabled
	PasswordEnv string `yaml:"password_env"`
	// OpTimeout caps every Redis call. Redis is on the hot path for cache and
	// rate limits, so a slow Redis must degrade to "skip", never to "slow".
	OpTimeout time.Duration `yaml:"op_timeout"`

	Password string `yaml:"-"`
}

type Cache struct {
	Enabled bool          `yaml:"enabled"`
	TTL     time.Duration `yaml:"ttl"`
	// Only requests with an explicit temperature <= this are cached. Unset
	// temperature means the provider default (1.0 for OpenAI), i.e. the client
	// expects varied answers, so it is never cached.
	MaxTemperature float64 `yaml:"max_temperature"`
}

type Auth struct {
	Enabled        bool          `yaml:"enabled"`
	DatabaseURLEnv string        `yaml:"database_url_env"`
	CacheTTL       time.Duration `yaml:"cache_ttl"` // how long a key lookup is reused; = revocation delay

	DatabaseURL string `yaml:"-"`
}

type Usage struct {
	Brokers []string `yaml:"brokers"` // empty = usage events disabled
	Topic   string   `yaml:"topic"`
	// MaxBuffered bounds the in-memory queue. If Kafka is down long enough to
	// fill it, new events are dropped (and counted) rather than blocking
	// requests or growing memory without limit.
	MaxBuffered int `yaml:"max_buffered"`
}

// Provider types. "openai" covers any OpenAI-compatible API (OpenAI itself,
// OpenRouter, Groq, vLLM, our mock) by changing base_url.
const (
	TypeOpenAI    = "openai"
	TypeAnthropic = "anthropic"
	TypeGemini    = "gemini"
)

// Routing strategies. Defined here (not in router) so config can validate them
// without an import cycle.
const (
	StrategyPriority = "priority" // fixed order, fail over down the list
	StrategyFastest  = "fastest"  // lowest predicted latency
	StrategyCheapest = "cheapest" // cheapest that meets the latency target
	StrategyWeighted = "weighted" // random split by weight (canaries, A/B)
)

func ValidStrategy(s string) bool {
	switch s {
	case StrategyPriority, StrategyFastest, StrategyCheapest, StrategyWeighted:
		return true
	}
	return false
}

type Provider struct {
	Type      string `yaml:"type"`
	BaseURL   string `yaml:"base_url"`
	APIKeyEnv string `yaml:"api_key_env"`
	// Timeout is a per-attempt "no progress" timeout: max time to first byte,
	// and max silence between streamed chunks. It is also the worst-case time
	// to detect a hung provider, so it directly bounds failover time.
	Timeout time.Duration `yaml:"timeout"`

	APIKey string `yaml:"-"` // resolved from APIKeyEnv at load; never serialized
}

type Model struct {
	Strategy        string `yaml:"strategy"`          // default strategy; header can override
	LatencyTargetMs int    `yaml:"latency_target_ms"` // for "cheapest"
	// HedgeAfter > 0 enables hedged requests (non-streaming only): if the first
	// attempt hasn't answered by then, a second one starts on the next
	// deployment and the first answer wins. Cuts tail latency, costs money:
	// both attempts can be billed.
	HedgeAfter  time.Duration `yaml:"hedge_after"`
	Deployments []Deployment  `yaml:"deployments"`
}

type Deployment struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"` // upstream model id sent to the provider
	// Prices in USD per 1M tokens, copied from provider pricing pages. Kept in
	// config rather than code because they change and differ per account.
	InputPricePerM  float64 `yaml:"input_price_per_m"`
	OutputPricePerM float64 `yaml:"output_price_per_m"`
	Priority        int     `yaml:"priority"` // lower = preferred (priority strategy)
	Weight          int     `yaml:"weight"`   // relative share (weighted strategy)
}

// Load reads, defaults, resolves secrets and validates. lookupEnv is injected
// so tests don't have to mutate the real process environment.
func Load(path string, lookupEnv func(string) (string, bool)) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	return Parse(raw, lookupEnv)
}

func Parse(raw []byte, lookupEnv func(string) (string, bool)) (*Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.resolveSecrets(lookupEnv); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func setDefault[T comparable](v *T, def T) {
	var zero T
	if *v == zero {
		*v = def
	}
}

func (c *Config) applyDefaults() {
	s := &c.Server
	setDefault(&s.Addr, ":8080")
	setDefault(&s.ReadHeaderTimeout, 5*time.Second) // slowloris protection
	setDefault(&s.IdleTimeout, 120*time.Second)
	setDefault(&s.ShutdownTimeout, 30*time.Second)
	setDefault(&s.MaxBodyBytes, 4<<20) // 4 MiB: generous for text chat, blocks abuse
	setDefault(&s.RequestTimeout, 120*time.Second)
	setDefault(&s.MetricsAddr, ":8081")

	r := &c.Router
	setDefault(&r.StatsWindow, 10*time.Second)
	setDefault(&r.SessionTTL, time.Hour)
	setDefault(&r.Breaker.ConsecutiveFailures, 5)
	setDefault(&r.Breaker.ErrorRate, 0.5)
	setDefault(&r.Breaker.MinRequests, 20)
	setDefault(&r.Breaker.Cooldown, 5*time.Second)
	setDefault(&r.Breaker.HalfOpenSuccesses, 3)
	setDefault(&r.RetryBudget.Ratio, 0.2)
	setDefault(&r.RetryBudget.MinPerSec, 10)

	setDefault(&c.Redis.OpTimeout, 50*time.Millisecond)
	setDefault(&c.Cache.TTL, time.Hour)
	setDefault(&c.Auth.CacheTTL, 30*time.Second)
	setDefault(&c.Usage.Topic, "llm-usage")
	setDefault(&c.Usage.MaxBuffered, 100_000)

	for name, p := range c.Providers {
		setDefault(&p.Timeout, 60*time.Second) // LLM calls are slow; tuned per provider
		c.Providers[name] = p
	}
	for name, m := range c.Models {
		setDefault(&m.Strategy, StrategyPriority)
		for i := range m.Deployments {
			setDefault(&m.Deployments[i].Weight, 1)
		}
		c.Models[name] = m
	}
}

func (c *Config) resolveSecrets(lookupEnv func(string) (string, bool)) error {
	var errs []error
	get := func(what, env string, dst *string) {
		if env == "" {
			return
		}
		v, ok := lookupEnv(env)
		if !ok || v == "" {
			// Missing secrets are an error, not a warning: a provider that will 401
			// on every call would just feed the circuit breaker garbage.
			errs = append(errs, fmt.Errorf("%s: env var %s is not set", what, env))
			return
		}
		*dst = v
	}
	for name, p := range c.Providers {
		get(fmt.Sprintf("provider %q", name), p.APIKeyEnv, &p.APIKey)
		c.Providers[name] = p
	}
	get("redis", c.Redis.PasswordEnv, &c.Redis.Password)
	if c.Auth.Enabled {
		get("auth", c.Auth.DatabaseURLEnv, &c.Auth.DatabaseURL)
	}
	return errors.Join(errs...)
}

func (c *Config) validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if len(c.Providers) == 0 {
		add("no providers configured")
	}
	for name, p := range c.Providers {
		switch p.Type {
		case TypeOpenAI, TypeAnthropic, TypeGemini:
		default:
			add("provider %q: unknown type %q", name, p.Type)
		}
		if p.BaseURL == "" {
			add("provider %q: base_url is required", name)
		}
		if p.Timeout < 0 {
			add("provider %q: timeout must be positive", name)
		}
	}
	if len(c.Models) == 0 {
		add("no models configured")
	}
	for name, m := range c.Models {
		if !ValidStrategy(m.Strategy) {
			add("model %q: unknown strategy %q", name, m.Strategy)
		}
		if m.LatencyTargetMs < 0 || m.HedgeAfter < 0 {
			add("model %q: latency_target_ms and hedge_after must be >= 0", name)
		}
		if len(m.Deployments) == 0 {
			add("model %q: no deployments", name)
		}
		for i, d := range m.Deployments {
			where := fmt.Sprintf("model %q deployment %d", name, i)
			if _, ok := c.Providers[d.Provider]; !ok {
				add("%s: unknown provider %q", where, d.Provider)
			}
			if d.Model == "" {
				add("%s: model is required", where)
			}
			if d.InputPricePerM < 0 || d.OutputPricePerM < 0 {
				add("%s: prices must be >= 0", where)
			}
			if d.Weight < 0 {
				add("%s: weight must be >= 0", where)
			}
		}
	}
	b := c.Router.Breaker
	if b.ErrorRate <= 0 || b.ErrorRate > 1 {
		add("router.breaker.error_rate must be in (0,1]")
	}
	if c.Router.RetryBudget.Ratio < 0 || c.Router.RetryBudget.MinPerSec < 0 {
		add("router.retry_budget values must be >= 0")
	}
	if c.Cache.Enabled && c.Redis.Addr == "" {
		add("cache.enabled requires redis.addr")
	}
	if c.Auth.Enabled {
		if c.Auth.DatabaseURLEnv == "" {
			add("auth.enabled requires auth.database_url_env")
		}
		if c.Redis.Addr == "" {
			add("auth.enabled requires redis.addr (rate limits and spend caps live in Redis)")
		}
	}
	return errors.Join(errs...)
}
