package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"llmgateway/internal/api"
	"llmgateway/internal/auth"
	"llmgateway/internal/cache"
	"llmgateway/internal/config"
	"llmgateway/internal/gateway"
	"llmgateway/internal/metrics"
	"llmgateway/internal/ratelimit"
	"llmgateway/internal/usage"
	"llmgateway/migrations"
)

func main() {
	// run() returns an error instead of calling os.Exit deep inside, so deferred
	// cleanups always execute and the startup path stays testable.
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfgPath := flag.String("config", "config.example.yaml", "path to config file")
	logLevel := flag.String("log-level", "info", "debug|info|warn|error")
	flag.Parse()

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		return err
	}
	// JSON logs: machine-parseable by Loki/CloudWatch, and request_id is a real
	// field you can filter on, not a substring.
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))
	slog.SetDefault(log)

	cfg, err := config.Load(*cfgPath, os.LookupEnv)
	if err != nil {
		return err
	}
	log.Info("config loaded", "path", *cfgPath, "providers", len(cfg.Providers), "models", len(cfg.Models),
		"redis", cfg.Redis.Addr != "", "cache", cfg.Cache.Enabled, "auth", cfg.Auth.Enabled, "usage", len(cfg.Usage.Brokers) > 0)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	m := metrics.New()
	rt, err := buildRouter(cfg, m, log)
	if err != nil {
		return err
	}

	// Optional dependencies. Each is nil when not configured.
	var rdb *redis.Client
	if cfg.Redis.Addr != "" {
		rdb = redis.NewClient(&redis.Options{
			Addr: cfg.Redis.Addr, Password: cfg.Redis.Password,
			// Pool sized for high concurrency: each in-flight request may hold a
			// connection briefly for cache + rate limit.
			PoolSize: 256, MinIdleConns: 16,
			ReadTimeout: cfg.Redis.OpTimeout, WriteTimeout: cfg.Redis.OpTimeout,
		})
		defer rdb.Close()
		// Not fatal if Redis is down at startup: cache and limits fail open, and
		// go-redis reconnects on its own.
		pctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		if err := rdb.Ping(pctx).Err(); err != nil {
			log.Warn("redis not reachable at startup; cache and limits will fail open", "err", err)
		}
		cancel()
	}

	var c *cache.Cache
	if cfg.Cache.Enabled {
		c = cache.New(rdb, cfg.Cache.TTL, cfg.Cache.MaxTemperature, cfg.Redis.OpTimeout)
	}

	var store *auth.Store
	var limiter *ratelimit.Limiter
	if cfg.Auth.Enabled {
		pool, err := pgxpool.New(ctx, cfg.Auth.DatabaseURL)
		if err != nil {
			return err
		}
		defer pool.Close()
		if err := waitFor(ctx, log, "postgres", func(ctx context.Context) error { return migrations.Apply(ctx, pool) }); err != nil {
			return err
		}
		store = auth.NewStore(pool, cfg.Auth.CacheTTL)
		limiter = ratelimit.New(rdb, cfg.Redis.OpTimeout)
	}

	var sink usage.Sink = usage.Noop{}
	if len(cfg.Usage.Brokers) > 0 {
		p, err := usage.NewProducer(cfg.Usage.Brokers, cfg.Usage.Topic, cfg.Usage.MaxBuffered, log, m.UsageEvents)
		if err != nil {
			return err
		}
		sink = p
		// Closed after the HTTP server drains (defers run in reverse), so events
		// from the last in-flight requests are flushed too.
		defer p.Close()
	}

	// Sticky routing is always available (opt-in per request via header).
	// Redis when configured, so all replicas share the mapping; otherwise
	// in memory, which is correct for a single instance.
	var sessions gateway.SessionStore = gateway.NewMemorySessions(cfg.Router.SessionTTL)
	if rdb != nil {
		sessions = gateway.NewRedisSessions(rdb, cfg.Router.SessionTTL, cfg.Redis.OpTimeout)
	}

	gw := gateway.New(rt, c, sessions, m, log)
	srv := api.NewServer(api.Deps{
		Log: log, Gateway: gw, Metrics: m, Usage: sink, Auth: store, Limiter: limiter,
		MaxBodyBytes: cfg.Server.MaxBodyBytes, RequestTimeout: cfg.Server.RequestTimeout, CacheOn: c != nil,
	})

	httpSrv := &http.Server{
		Addr:              cfg.Server.Addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout,
		IdleTimeout:       cfg.Server.IdleTimeout,
	}
	adminSrv := &http.Server{Addr: cfg.Server.MetricsAddr, Handler: srv.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 2)
	for _, s := range []*http.Server{httpSrv, adminSrv} {
		go func() {
			log.Info("listening", "addr", s.Addr)
			if err := s.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
		}()
	}
	srv.SetReady(true)

	select {
	case err := <-errCh:
		return err // e.g. port already in use
	case <-ctx.Done():
	}

	// Graceful shutdown, in Kubernetes order:
	//  1. Fail readiness so the load balancer stops routing new requests here.
	//  2. Wait briefly: endpoint removal propagates asynchronously, so for a
	//     moment traffic still arrives. Closing the listener now would refuse it.
	//  3. Shutdown: stop accepting, wait for in-flight requests (incl. streams)
	//     up to the timeout, then exit. Deferred closes flush usage events.
	stop() // a second Ctrl-C now kills immediately instead of being swallowed
	log.Info("shutting down")
	srv.SetReady(false)
	time.Sleep(drainDelay())

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	_ = adminSrv.Shutdown(shutdownCtx)
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	log.Info("shutdown complete")
	return nil
}

// waitFor retries a dependency at startup (compose starts containers in
// parallel; Postgres may need a few seconds).
func waitFor(ctx context.Context, log *slog.Logger, what string, f func(context.Context) error) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := f(ctx)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		log.Warn("waiting for "+what, "err", err)
		time.Sleep(time.Second)
	}
}

// drainDelay is 0 locally (fast Ctrl-C) and set via env in Kubernetes, where it
// should be a few seconds, longer than the readiness probe period.
func drainDelay() time.Duration {
	d, _ := time.ParseDuration(os.Getenv("GATEWAY_DRAIN_DELAY"))
	return d
}

func msDur(ms int) time.Duration { return time.Duration(ms) * time.Millisecond }
