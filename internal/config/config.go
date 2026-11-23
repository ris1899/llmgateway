// Package config loads and validates the gateway's YAML config. Secrets stay
// out of the file: fields ending in _env name an env var resolved at load
// time. Unknown fields are a load error, and every infrastructure dependency
// (redis, usage brokers) is optional so `make run` works with zero deps.
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
	// Models maps a public model name to the deployments that can serve it;
	// several deployments make it a "model group" the router picks between.
	Models map[string]Model `yaml:"models"`
}

type Server struct {
	Addr              string        `yaml:"addr"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	// no WriteTimeout field: it's a hard cap on the whole response and would
	// kill long SSE streams; deadlines are enforced per-request in the handler instead
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	MaxBodyBytes    int64         `yaml:"max_body_bytes"`
	RequestTimeout  time.Duration `yaml:"request_timeout"` // bounds a request across all failover attempts
	MetricsAddr     string        `yaml:"metrics_addr"`    // separate port, never exposed through the public LB
}

type Router struct {
	Breaker     Breaker       `yaml:"breaker"`
	RetryBudget RetryBudget   `yaml:"retry_budget"`
	StatsWindow time.Duration `yaml:"stats_window"` // how far back error rates look; short reacts fast but noisy
	// how long an idle conversation stays sticky; provider prompt caches expire
	// within minutes anyway, so remembering longer buys nothing
	SessionTTL time.Duration `yaml:"session_ttl"`
}

type Breaker struct {
	ConsecutiveFailures int           `yaml:"consecutive_failures"` // trips fast on a hard-down provider regardless of volume
	ErrorRate           float64       `yaml:"error_rate"`           // or trips on error share over the stats window
	MinRequests         int           `yaml:"min_requests"`         // don't judge error rate on tiny samples
	Cooldown            time.Duration `yaml:"cooldown"`             // before letting probe requests test an open breaker
	HalfOpenSuccesses   int           `yaml:"half_open_successes"`  // consecutive successes needed to close again
}

type RetryBudget struct {
	Ratio     float64 `yaml:"ratio"`       // extra attempts allowed per request; 0.2 = retries add at most ~20% load
	MinPerSec float64 `yaml:"min_per_sec"` // lets low-traffic gateways still retry
}

type Redis struct {
	Addr        string        `yaml:"addr"` // empty = Redis features disabled
	PasswordEnv string        `yaml:"password_env"`
	OpTimeout   time.Duration `yaml:"op_timeout"` // caps every Redis call; a slow Redis must degrade to skip, not slow

	Password string `yaml:"-"`
}

type Cache struct {
	Enabled bool          `yaml:"enabled"`
	TTL     time.Duration `yaml:"ttl"`
	// only requests with an explicit temperature <= this are cached; unset
	// temperature means the client wants varied answers, so never cache it
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
	// per-attempt no-progress timeout: max time to first byte or between
	// streamed chunks, so it also bounds failover time
	Timeout time.Duration `yaml:"timeout"`

	APIKey string `yaml:"-"` // resolved from APIKeyEnv at load; never serialized
}

type Model struct {
	Strategy        string `yaml:"strategy"`          // default strategy; header can override
	LatencyTargetMs int    `yaml:"latency_target_ms"` // for "cheapest"
	// > 0 enables hedged requests (non-streaming only): a second attempt starts
	// on the next deployment if the first hasn't answered; cuts tail latency, costs money
	HedgeAfter  time.Duration `yaml:"hedge_after"`
	Deployments []Deployment  `yaml:"deployments"`
}

type Deployment struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"` // upstream model id sent to the provider
	// USD per 1M tokens; kept in config since prices change and differ per account
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
			// missing secret is an error, not a warning: a 401'ing provider just feeds the breaker garbage
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
		seen := make(map[string]bool, len(m.Deployments))
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
			// same (provider, model) twice in a group is almost always a copy-paste mistake
			key := d.Provider + "/" + d.Model
			if d.Provider != "" && d.Model != "" {
				if seen[key] {
					add("%s: duplicate deployment %q in model %q", where, key, name)
				}
				seen[key] = true
			}
		}
	}
	if c.Server.ReadHeaderTimeout < 0 || c.Server.IdleTimeout < 0 || c.Server.ShutdownTimeout < 0 ||
		c.Server.RequestTimeout < 0 || c.Server.MaxBodyBytes < 0 {
		add("server timeouts and max_body_bytes must be >= 0")
	}
	b := c.Router.Breaker
	if b.ErrorRate <= 0 || b.ErrorRate > 1 {
		add("router.breaker.error_rate must be in (0,1]")
	}
	if b.ConsecutiveFailures < 0 || b.MinRequests < 0 || b.Cooldown < 0 || b.HalfOpenSuccesses < 0 {
		add("router.breaker values must be >= 0")
	}
	if c.Router.RetryBudget.Ratio < 0 || c.Router.RetryBudget.MinPerSec < 0 {
		add("router.retry_budget values must be >= 0")
	}
	if c.Router.StatsWindow < 0 || c.Router.SessionTTL < 0 {
		add("router.stats_window and router.session_ttl must be >= 0")
	}
	if c.Redis.OpTimeout < 0 {
		add("redis.op_timeout must be >= 0")
	}
	if c.Cache.TTL < 0 || c.Cache.MaxTemperature < 0 {
		add("cache.ttl and cache.max_temperature must be >= 0")
	}
	if c.Auth.CacheTTL < 0 {
		add("auth.cache_ttl must be >= 0")
	}
	if c.Usage.MaxBuffered < 0 {
		add("usage.max_buffered must be >= 0")
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
