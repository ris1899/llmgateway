package main

import (
	"fmt"
	"log/slog"

	"llmgateway/internal/config"
	"llmgateway/internal/metrics"
	"llmgateway/internal/provider"
	"llmgateway/internal/provider/anthropic"
	"llmgateway/internal/provider/openai"
	"llmgateway/internal/router"
)

// buildRouter turns config into live objects: one adapter per provider (all
// sharing one HTTP client and connection pool) and, per model group, its
// deployments with their own stats and circuit breaker.
func buildRouter(cfg *config.Config, m *metrics.Metrics, log *slog.Logger) (*router.Router, error) {
	hc := provider.NewHTTPClient()
	providers := make(map[string]provider.Provider, len(cfg.Providers))
	for name, p := range cfg.Providers {
		switch p.Type {
		case config.TypeOpenAI:
			providers[name] = openai.New(name, p.BaseURL, p.APIKey, p.Timeout, hc)
		case config.TypeAnthropic:
			providers[name] = anthropic.New(name, p.BaseURL, p.APIKey, p.Timeout, hc)
		default:
			return nil, fmt.Errorf("provider %q: type %q has no adapter yet", name, p.Type)
		}
	}

	groups := make(map[string]*router.Group, len(cfg.Models))
	for name, mc := range cfg.Models {
		g := &router.Group{
			Name: name, Strategy: mc.Strategy, HedgeAfter: mc.HedgeAfter,
			LatencyTarget: msDur(mc.LatencyTargetMs),
		}
		for _, d := range mc.Deployments {
			id := d.Provider + "/" + d.Model
			// Breaker state is per (group, deployment): the same upstream model in
			// two groups shares a provider but gets its own health view, which
			// keeps one group's traffic pattern from flipping another's breaker.
			label := name + ":" + id
			gauge := m.BreakerState.WithLabelValues(label)
			breaker := router.NewBreaker(cfg.Router.Breaker, func(from, to router.State) {
				gauge.Set(float64(to))
				m.BreakerTrips.WithLabelValues(label, to.String()).Inc()
				log.Warn("circuit breaker", "group", name, "deployment", id, "from", from.String(), "to", to.String())
			})
			g.Deployments = append(g.Deployments, &router.Deployment{
				ID: id, Group: name, Provider: providers[d.Provider], Model: d.Model,
				InputPricePerM: d.InputPricePerM, OutputPricePerM: d.OutputPricePerM,
				Priority: d.Priority, Weight: d.Weight,
				Stats: router.NewStats(cfg.Router.StatsWindow), Breaker: breaker,
			})
		}
		groups[name] = g
	}
	budget := router.NewBudget(cfg.Router.RetryBudget.Ratio, cfg.Router.RetryBudget.MinPerSec)
	return router.New(groups, budget), nil
}
