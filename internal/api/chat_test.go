package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"llmgateway/internal/config"
	"llmgateway/internal/gateway"
	"llmgateway/internal/llm"
	"llmgateway/internal/metrics"
	"llmgateway/internal/provider"
	"llmgateway/internal/router"
)

func doChat(h http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func newChatTestServer(providers ...*fakeProvider) (*Server, http.Handler) {
	pp := make([]provider.Provider, len(providers))
	for i, p := range providers {
		pp[i] = p
	}
	gw := newTestGateway(pp...)
	s := NewServer(Deps{Log: slog.New(slog.DiscardHandler), Gateway: gw, MaxBodyBytes: 1 << 20})
	return s, s.Handler()
}

func TestHandleChatUnsupportedParameterIs400(t *testing.T) {
	t.Parallel()
	_, h := newChatTestServer(&fakeProvider{err: &llm.Error{Kind: llm.KindUnavailable}}) // would 502 if it reached the provider
	rec := doChat(h, `{"model":"g","seed":7,"messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", rec.Code, rec.Body)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "unsupported parameter: seed") || strings.Contains(body, "invalid JSON") {
		t.Fatalf("body = %s, want an \"unsupported parameter: seed\" message without \"invalid JSON\"", body)
	}
}

func TestHandleChatSuccess(t *testing.T) {
	t.Parallel()
	resp := &llm.ChatResponse{ID: "x", Content: "hello", FinishReason: "stop", Usage: llm.Usage{InputTokens: 5, OutputTokens: 3}}
	_, h := newChatTestServer(&fakeProvider{resp: resp})

	rec := doChat(h, chatBody(false))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if rec.Header().Get(HeaderProvider) == "" {
		t.Error("missing " + HeaderProvider)
	}
	if rec.Header().Get(HeaderAttempts) != "1" {
		t.Errorf(HeaderAttempts+" = %q, want 1", rec.Header().Get(HeaderAttempts))
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["model"] == nil {
		t.Errorf("response missing model field: %s", rec.Body)
	}
}

func TestHandleChatFailoverSetsSecondProviderHeader(t *testing.T) {
	t.Parallel()
	_, h := newChatTestServer(
		&fakeProvider{err: &llm.Error{Kind: llm.KindUnavailable}},
		&fakeProvider{resp: &llm.ChatResponse{ID: "x", FinishReason: "stop"}},
	)
	rec := doChat(h, chatBody(false))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body)
	}
	if rec.Header().Get(HeaderAttempts) != "2" {
		t.Errorf(HeaderAttempts+" = %q, want 2", rec.Header().Get(HeaderAttempts))
	}
}

func TestHandleChatFailureHeadersAndStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantRetry  bool
	}{
		{"bad_request", &llm.Error{Kind: llm.KindBadRequest}, http.StatusBadRequest, false},
		{"auth", &llm.Error{Kind: llm.KindAuth}, http.StatusBadGateway, false},
		{"unavailable", &llm.Error{Kind: llm.KindUnavailable}, http.StatusBadGateway, false},
		{"timeout", &llm.Error{Kind: llm.KindTimeout}, http.StatusGatewayTimeout, false},
		{"not_found", &llm.Error{Kind: llm.KindNotFound}, http.StatusBadGateway, false},
		{"rate_limited_subsecond", &llm.Error{Kind: llm.KindRateLimited, RetryAfter: 200 * time.Millisecond}, http.StatusTooManyRequests, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, h := newChatTestServer(&fakeProvider{err: tt.err})
			rec := doChat(h, chatBody(false))
			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d, body=%s", rec.Code, tt.wantStatus, rec.Body)
			}
			if got := rec.Header().Get(HeaderAttempts); got != "1" {
				t.Errorf(HeaderAttempts+" = %q, want 1 (regression: failed requests must report attempts)", got)
			}
			if got := rec.Header().Get(HeaderProvider); got == "" {
				t.Error(HeaderProvider + " missing on a failed request that made an attempt")
			}
			if tt.wantRetry {
				ra, err := strconv.Atoi(rec.Header().Get("Retry-After"))
				if err != nil || ra < 1 {
					t.Errorf("Retry-After = %q, want an integer >= 1 (clamped, never 0)", rec.Header().Get("Retry-After"))
				}
			}
		})
	}
}

func TestHandleChatNoHealthyUpstream(t *testing.T) {
	t.Parallel()
	cfg := generousBreaker()
	cfg.ConsecutiveFailures = 1
	dep := &router.Deployment{ID: "fake/dep0", Group: testModel, Provider: &fakeProvider{}, Model: "m",
		Stats: router.NewStats(time.Second), Breaker: router.NewBreaker(cfg, nil)}
	dep.Breaker.Report(time.Now(), false, router.Failure, 1, 1) // trip it before any request
	grp := &router.Group{Name: testModel, Strategy: config.StrategyPriority, Deployments: []*router.Deployment{dep}}
	r := router.New(map[string]*router.Group{testModel: grp}, router.NewBudget(1, 1000))
	gw := gateway.New(r, nil, nil, metrics.New(), slog.New(slog.DiscardHandler))
	s := NewServer(Deps{Log: slog.New(slog.DiscardHandler), Gateway: gw, MaxBodyBytes: 1 << 20})

	rec := doChat(s.Handler(), chatBody(false))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body=%s", rec.Code, rec.Body)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "1" {
		t.Errorf("Retry-After = %q, want 1", ra)
	}
	if got := rec.Header().Get(HeaderAttempts); got != "" {
		t.Errorf(HeaderAttempts+" = %q, want unset: no attempt was made", got)
	}
}

func TestHandleChatUnknownModel(t *testing.T) {
	t.Parallel()
	_, h := newChatTestServer(&fakeProvider{})
	rec := doChat(h, `{"model":"does-not-exist","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404, body=%s", rec.Code, rec.Body)
	}
}
