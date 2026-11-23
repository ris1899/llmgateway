// Package api is the HTTP edge: routing, middleware, OpenAI-shaped errors and
// SSE. It holds no routing logic; handlers decode, call into the gateway
// package, and encode.
package api

import (
	"log/slog"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"llmgateway/internal/auth"
	"llmgateway/internal/gateway"
	"llmgateway/internal/metrics"
	"llmgateway/internal/ratelimit"
	"llmgateway/internal/usage"
)

type Deps struct {
	Log          *slog.Logger
	Gateway      *gateway.Gateway
	Metrics      *metrics.Metrics
	Usage        usage.Sink
	Auth         *auth.Store        // nil = auth disabled
	Limiter      *ratelimit.Limiter // nil = no rate limits / spend caps
	MaxBodyBytes int64
	// RequestTimeout bounds a non-streaming request end to end across all
	// failover attempts. Streams are bounded by the per-attempt no-progress
	// deadline instead (D15), since they can legitimately run for minutes.
	RequestTimeout time.Duration
	CacheOn        bool
}

type Server struct {
	log          *slog.Logger
	gw           *gateway.Gateway
	m            *metrics.Metrics
	usage        usage.Sink
	auth         *auth.Store
	limiter      *ratelimit.Limiter
	maxBodyBytes int64
	reqTimeout   time.Duration
	cacheOn      bool
	ready        atomic.Bool
	mux          *http.ServeMux
}

func NewServer(d Deps) *Server {
	if d.Metrics == nil {
		d.Metrics = metrics.New()
	}
	if d.Usage == nil {
		d.Usage = usage.Noop{}
	}
	s := &Server{log: d.Log, gw: d.Gateway, m: d.Metrics, usage: d.Usage, auth: d.Auth,
		limiter: d.Limiter, maxBodyBytes: d.MaxBodyBytes, reqTimeout: d.RequestTimeout, cacheOn: d.CacheOn, mux: http.NewServeMux()}
	// Go 1.22+ ServeMux supports method + path patterns, so no router dependency.
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.Handle("POST /v1/chat/completions", s.withAuth(http.HandlerFunc(s.handleChat)))
	s.mux.Handle("GET /v1/models", s.withAuth(http.HandlerFunc(s.handleModels)))
	return s
}

// Handler returns the public middleware chain. Order matters: request id is
// outermost so the access log and panic log both see it.
func (s *Server) Handler() http.Handler {
	var h http.Handler = s.mux
	h = withRecover(s.log, h)
	h = withAccessLog(s.log, h)
	h = withRequestID(h)
	return h
}

// AdminHandler serves metrics, deployment health and pprof on a separate port.
// Never exposed through the public listener: pprof can leak memory contents
// and /admin shows internal topology.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.m.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /admin/deployments", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, s.gw.Status())
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// SetReady flips readiness. main sets it true once dependencies are up and
// false at the start of shutdown, so the load balancer stops sending new
// traffic while in-flight requests drain.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Liveness vs readiness: /healthz answers "is the process alive?" and must not
// depend on Redis/Kafka, or a Redis blip would make Kubernetes restart every
// pod at once. /readyz answers "should I get traffic?" and is allowed to fail.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	if !s.ready.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("not ready"))
		return
	}
	_, _ = w.Write([]byte("ready"))
}

// errorBody mirrors OpenAI's error envelope so existing OpenAI SDKs parse our
// errors without changes.
type errorBody struct {
	Error struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error"`
}

func writeError(w http.ResponseWriter, status int, typ, msg string) {
	var b errorBody
	b.Error.Message = msg
	b.Error.Type = typ
	writeJSON(w, status, b)
}
