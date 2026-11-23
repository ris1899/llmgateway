package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"llmgateway/internal/auth"
	"llmgateway/internal/config"
	"llmgateway/internal/gateway"
	"llmgateway/internal/llm"
	"llmgateway/internal/router"
	"llmgateway/internal/usage"
)

const (
	HeaderProvider = "X-Gateway-Provider"
	HeaderAttempts = "X-Gateway-Attempts"
	HeaderUpstream = "X-Gateway-Upstream-Ms"
	HeaderOverhead = "X-Gateway-Overhead-Ms"
	HeaderCost     = "X-Gateway-Cost-Usd"
	HeaderCache    = "X-Gateway-Cache"
	HeaderHedged   = "X-Gateway-Hedged"
	HeaderSticky   = "X-Gateway-Sticky" // new | hit | moved

	HeaderStrategy      = "X-Gateway-Strategy"
	HeaderLatencyTarget = "X-Gateway-Latency-Target-Ms"
	HeaderNoCache       = "X-Gateway-No-Cache"
	HeaderSession       = "X-Gateway-Session" // client's conversation id
)

// header > API key default > model default
func options(r *http.Request) (gateway.Options, error) {
	var o gateway.Options
	if k := auth.FromContext(r.Context()); k != nil {
		o.Policy.Strategy = k.DefaultStrategy
	}
	if s := r.Header.Get(HeaderStrategy); s != "" {
		if !config.ValidStrategy(s) {
			return o, badRequest("unknown " + HeaderStrategy + " " + strconv.Quote(s))
		}
		o.Policy.Strategy = s
	}
	if v := r.Header.Get(HeaderLatencyTarget); v != "" {
		ms, err := strconv.Atoi(v)
		if err != nil || ms < 0 {
			return o, badRequest(HeaderLatencyTarget + " must be a non-negative integer")
		}
		o.Policy.LatencyTarget = time.Duration(ms) * time.Millisecond
	}
	o.NoCache = strings.EqualFold(r.Header.Get(HeaderNoCache), "true") ||
		strings.Contains(r.Header.Get("Cache-Control"), "no-cache")
	if sid := r.Header.Get(HeaderSession); sid != "" {
		if len(sid) > 256 {
			return o, badRequest(HeaderSession + " must be at most 256 characters")
		}
		keyID := "anonymous"
		if k := auth.FromContext(r.Context()); k != nil {
			keyID = k.ID
		}
		o.Session = keyID + "\x00" + sid
	}
	return o, nil
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	r.Body = http.MaxBytesReader(w, r.Body, s.maxBodyBytes)

	var wire chatRequest
	if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "invalid_request_error", "request body too large")
			return
		}
		var br badRequest
		if errors.As(err, &br) {
			writeError(w, http.StatusBadRequest, "invalid_request_error", br.Error())
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid JSON: "+err.Error())
		return
	}
	req, err := wire.toLLM()
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	opt, err := options(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	if req.Stream {
		includeUsage := wire.StreamOptions != nil && wire.StreamOptions.IncludeUsage
		s.streamChat(w, r, req, opt, includeUsage, start)
		return
	}

	ctx := r.Context()
	if s.reqTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.reqTimeout)
		defer cancel()
	}
	res, err := s.gw.Chat(ctx, req, opt)
	if err != nil {
		status := s.writeUpstreamError(w, r, err)
		s.finish(r, req, start, status, llm.AsError(err).Kind.String(), failureOutcome(err), 0)
		return
	}
	resp := res.Response
	overhead := time.Since(start) - res.Upstream // everything except waiting on providers
	cost := 0.0
	if res.Deployment != nil && !res.CacheHit {
		cost = res.Deployment.Cost(resp.Usage)
	}
	h := w.Header()
	h.Set(HeaderProvider, res.DeployID)
	h.Set(HeaderAttempts, strconv.Itoa(res.Attempts))
	h.Set(HeaderUpstream, ms(res.Upstream))
	h.Set(HeaderOverhead, ms(overhead))
	h.Set(HeaderCost, strconv.FormatFloat(cost, 'f', 8, 64))
	h.Set(HeaderCache, cacheLabel(res.CacheHit, req, opt, s.cacheOn))
	if res.Hedged {
		h.Set(HeaderHedged, "true")
	}
	if res.Sticky != "" {
		h.Set(HeaderSticky, res.Sticky)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"id": resp.ID, "object": "chat.completion", "created": time.Now().Unix(), "model": resp.Model,
		"choices": []map[string]any{{
			"index":         0,
			"message":       map[string]string{"role": "assistant", "content": resp.Content},
			"finish_reason": resp.FinishReason,
		}},
		"usage": map[string]int{
			"prompt_tokens": resp.Usage.InputTokens, "completion_tokens": resp.Usage.OutputTokens,
			"total_tokens": resp.Usage.InputTokens + resp.Usage.OutputTokens,
		},
	})

	s.m.Overhead.WithLabelValues("false").Observe(overhead.Seconds())
	s.finish(r, req, start, http.StatusOK, "ok", &outcome{
		deployID: res.DeployID, d: res.Deployment, usage: resp.Usage, cost: cost,
		attempts: res.Attempts, cacheHit: res.CacheHit,
	}, 0)
}

type outcome struct {
	deployID string
	d        *router.Deployment
	usage    llm.Usage
	cost     float64
	attempts int
	cacheHit bool
}

// nil if the error happened before any attempt was made
func failureOutcome(err error) *outcome {
	var f *gateway.Failure
	if !errors.As(err, &f) {
		return nil
	}
	return &outcome{deployID: f.DeployID, d: f.Deployment, attempts: f.Attempts}
}

// single exit point for accounting: metrics, spend, usage event, log line
func (s *Server) finish(r *http.Request, req *llm.ChatRequest, start time.Time, status int, kind string, o *outcome, ttft time.Duration) {
	dur := time.Since(start)
	cache := "false"
	if o != nil && o.cacheHit {
		cache = "true"
	}
	s.m.Requests.WithLabelValues(req.Model, strconv.Itoa(status), cache).Inc()
	lat := dur
	if req.Stream && ttft > 0 {
		lat = ttft // streams: latency that matters to users is TTFT
	}
	if status == http.StatusOK {
		s.m.RequestLatency.WithLabelValues(req.Model, strconv.FormatBool(req.Stream)).Observe(lat.Seconds())
	}

	key := auth.FromContext(r.Context())
	keyID := "anonymous"
	if key != nil {
		keyID = key.ID
	}
	ev := usage.Event{
		EventID: usage.NewEventID(), TS: start.UTC(), RequestID: RequestID(r.Context()),
		KeyID: keyID, Model: req.Model, Status: kind, Stream: req.Stream,
		LatencyMs: msf(dur), TTFTMs: msf(ttft),
	}
	if o != nil {
		ev.Deployment, ev.CacheHit, ev.Attempts = o.deployID, o.cacheHit, o.attempts
		ev.InputTokens, ev.OutputTokens, ev.Estimated, ev.CostUSD = o.usage.InputTokens, o.usage.OutputTokens, o.usage.Estimated, o.cost
		if o.d != nil && !o.cacheHit {
			s.m.Tokens.WithLabelValues(o.deployID, "input").Add(float64(o.usage.InputTokens))
			s.m.Tokens.WithLabelValues(o.deployID, "output").Add(float64(o.usage.OutputTokens))
			s.m.CostUSD.WithLabelValues(o.deployID).Add(o.cost)
		}
		if key != nil && s.limiter != nil && o.cost > 0 {
			if err := s.limiter.AddSpend(r.Context(), key.ID, o.cost); err != nil {
				s.log.Warn("spend update failed", "request_id", ev.RequestID, "err", err)
			}
		}
	}
	s.usage.Emit(ev)
	s.log.Info("chat", "request_id", ev.RequestID, "key", keyID, "model", req.Model,
		"deployment", ev.Deployment, "stream", req.Stream, "status", status, "kind", kind,
		"cache_hit", ev.CacheHit, "attempts", ev.Attempts, "input_tokens", ev.InputTokens,
		"output_tokens", ev.OutputTokens, "cost_usd", ev.CostUSD, "duration", dur)
}

func cacheLabel(hit bool, req *llm.ChatRequest, opt gateway.Options, cacheOn bool) string {
	switch {
	case hit:
		return "hit"
	case !cacheOn || opt.NoCache || req.Temperature == nil:
		return "skip"
	default:
		return "miss"
	}
}

// Auth kind maps to 502: a provider 401 means our provider key is broken.
func (s *Server) writeUpstreamError(w http.ResponseWriter, r *http.Request, err error) int {
	var f *gateway.Failure
	if errors.As(err, &f) {
		w.Header().Set(HeaderAttempts, strconv.Itoa(f.Attempts))
		if f.DeployID != "" {
			w.Header().Set(HeaderProvider, f.DeployID)
		}
		if f.Hedged {
			w.Header().Set(HeaderHedged, "true")
		}
	}
	switch {
	case errors.Is(err, gateway.ErrUnknownModel):
		writeError(w, http.StatusNotFound, "model_not_found", "model not found; see GET /v1/models")
		return http.StatusNotFound
	case errors.Is(err, gateway.ErrNoHealthy):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "no_healthy_upstream", "all providers for this model are unavailable")
		return http.StatusServiceUnavailable
	}
	e := llm.AsError(err)
	status, typ := http.StatusBadGateway, "upstream_error"
	switch e.Kind {
	case llm.KindBadRequest:
		status, typ = http.StatusBadRequest, "invalid_request_error"
	case llm.KindContentFilter:
		status, typ = http.StatusBadRequest, "content_filter"
	case llm.KindRateLimited:
		status, typ = http.StatusTooManyRequests, "rate_limit_error"
		if e.RetryAfter > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(max(1, int(e.RetryAfter.Seconds()+0.999)))) // round up, floor at 1s
		}
	case llm.KindTimeout:
		status, typ = http.StatusGatewayTimeout, "timeout"
	case llm.KindNotFound:
		typ = "upstream_model_not_found" // gateway config is wrong, not the client's request
	case llm.KindCanceled:
		return 499 // client closed request (nginx convention); nobody to answer
	}
	s.log.Warn("upstream failed", "request_id", RequestID(r.Context()), "provider", e.Provider,
		"kind", e.Kind.String(), "status", e.Status, "err", e.Err)
	// log only: raw upstream errors may contain internal hostnames or echoed request content
	writeError(w, status, typ, fmt.Sprintf("upstream %s failed: %s", e.Provider, e.Kind))
	return status
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	data := []map[string]any{}
	for _, m := range s.gw.Models() {
		data = append(data, map[string]any{"id": m, "object": "model", "created": 0, "owned_by": "llmgateway"})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func ms(d time.Duration) string {
	return strconv.FormatFloat(msf(d), 'f', 3, 64)
}

func msf(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
