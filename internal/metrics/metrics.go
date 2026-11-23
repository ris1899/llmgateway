// Package metrics registers every Prometheus metric in one place. Labels are bounded sets only, never key/request ids.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	Registry *prometheus.Registry

	Requests       *prometheus.CounterVec   // model, status, cache
	RequestLatency *prometheus.HistogramVec // model, stream
	Overhead       *prometheus.HistogramVec // stream
	Attempts       *prometheus.CounterVec   // deployment, outcome
	UpstreamTTFT   *prometheus.HistogramVec // deployment
	UpstreamTime   *prometheus.HistogramVec // deployment
	BreakerState   *prometheus.GaugeVec     // deployment
	BreakerTrips   *prometheus.CounterVec   // deployment, to
	Tokens         *prometheus.CounterVec   // deployment, direction
	CostUSD        *prometheus.CounterVec   // deployment
	Cache          *prometheus.CounterVec   // result: hit, miss, skip, error
	RateLimited    *prometheus.CounterVec   // reason: rate, spend, error
	RetryBudget    *prometheus.CounterVec   // result: allowed, exhausted
	Hedges         *prometheus.CounterVec   // result: started, won
	Sticky         *prometheus.CounterVec   // result: new, hit, moved
	UsageEvents    *prometheus.CounterVec   // result: queued, delivered, failed, dropped
	ActiveStreams  prometheus.Gauge
}

var (
	overheadBuckets = []float64{.0001, .00025, .0005, .001, .002, .003, .005, .01, .025, .05}
	latencyBuckets  = []float64{.001, .003, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}
)

func New() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { reg.MustRegister(c) }

	m := &Metrics{Registry: reg}
	m.Requests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_requests_total", Help: "Client requests by final status."}, []string{"model", "status", "cache"})
	m.RequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "llmgw_request_duration_seconds", Help: "End-to-end request latency (non-stream) or time to first token (stream).",
		Buckets: latencyBuckets}, []string{"model", "stream"})
	m.Overhead = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "llmgw_overhead_seconds", Help: "Time spent in the gateway itself, excluding provider time.",
		Buckets: overheadBuckets}, []string{"stream"})
	m.Attempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_upstream_attempts_total", Help: "Upstream attempts by outcome (ok or error kind)."}, []string{"deployment", "outcome"})
	m.UpstreamTTFT = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "llmgw_upstream_ttft_seconds", Help: "Provider time to first token (streams).",
		Buckets: latencyBuckets}, []string{"deployment"})
	m.UpstreamTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "llmgw_upstream_duration_seconds", Help: "Provider response time (non-stream, successful).",
		Buckets: latencyBuckets}, []string{"deployment"})
	m.BreakerState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "llmgw_breaker_state", Help: "Circuit breaker state: 0 closed, 1 open, 2 half-open."}, []string{"deployment"})
	m.BreakerTrips = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_breaker_transitions_total", Help: "Breaker state transitions."}, []string{"deployment", "to"})
	m.Tokens = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_tokens_total", Help: "Tokens processed."}, []string{"deployment", "direction"})
	m.CostUSD = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_cost_usd_total", Help: "Estimated spend in USD from configured prices."}, []string{"deployment"})
	m.Cache = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_cache_lookups_total", Help: "Cache lookups by result."}, []string{"result"})
	m.RateLimited = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_limit_rejections_total", Help: "Requests rejected by rate limit or spend cap."}, []string{"reason"})
	m.RetryBudget = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_retry_budget_total", Help: "Retry budget decisions."}, []string{"result"})
	m.Hedges = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_hedges_total", Help: "Hedged requests started / won."}, []string{"result"})
	m.Sticky = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_sticky_sessions_total", Help: "Sticky routing outcomes: new conversation, same deployment, moved."}, []string{"result"})
	m.UsageEvents = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "llmgw_usage_events_total", Help: "Usage events by delivery result."}, []string{"result"})
	m.ActiveStreams = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "llmgw_active_streams", Help: "Streaming responses currently open."})

	for _, c := range []prometheus.Collector{m.Requests, m.RequestLatency, m.Overhead, m.Attempts,
		m.UpstreamTTFT, m.UpstreamTime, m.BreakerState, m.BreakerTrips, m.Tokens, m.CostUSD, m.Cache,
		m.RateLimited, m.RetryBudget, m.Hedges, m.Sticky, m.UsageEvents, m.ActiveStreams} {
		f(c)
	}
	return m
}
