package main

// End-to-end: config.Load -> buildRouter -> gateway -> api handler -> real
// provider adapters over HTTP -> mockprovider. The only thing mocked is the
// upstream LLM, so this catches wiring mistakes unit tests can't see.

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"llmgateway/internal/api"
	"llmgateway/internal/config"
	"llmgateway/internal/gateway"
	"llmgateway/internal/metrics"
	"llmgateway/internal/mockprovider"
)

// newMock starts an in-process mock LLM server and returns its base URL plus
// a function to change its behavior via the admin endpoint.
func newMock(t *testing.T, name string) (baseURL string, setBehavior func(t *testing.T, body string)) {
	t.Helper()
	srv, err := mockprovider.New(name, mockprovider.DefaultBehavior())
	if err != nil {
		t.Fatalf("mockprovider.New: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	set := func(t *testing.T, body string) {
		t.Helper()
		req, err := http.NewRequest(http.MethodPatch, ts.URL+"/admin/behavior", strings.NewReader(body))
		if err != nil {
			t.Fatalf("new PATCH request: %v", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("PATCH /admin/behavior: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("PATCH /admin/behavior status = %d, body = %s", resp.StatusCode, b)
		}
	}
	return ts.URL, set
}

// testStack is everything one subtest needs: an HTTP server exercising the
// real wiring, plus the underlying gateway for breaker-state assertions that
// don't go over HTTP.
type testStack struct {
	ts *httptest.Server
	gw *gateway.Gateway
}

func (st *testStack) breakerState(id string) string {
	for _, d := range st.gw.Status() {
		if d.ID == id {
			return d.Breaker
		}
	}
	return "<unknown deployment>"
}

// stackConfig is the knobs each scenario tunes: provider URLs, per-attempt
// timeout, and breaker tuning kept tiny so trips/cooldowns fit the test budget.
type stackConfig struct {
	PrimaryURL     string
	SecondaryURL   string
	AnthropicURL   string
	ProviderTimeMs int
	CooldownMs     int
}

const cfgTemplate = `
server:
  request_timeout: 2s
router:
  breaker:
    consecutive_failures: 2
    error_rate: 0.9
    min_requests: 1000
    cooldown: %[5]dms
    half_open_successes: 1
  retry_budget:
    ratio: 100
    min_per_sec: 1000
providers:
  primary:
    type: openai
    base_url: %[1]q
    timeout: %[4]dms
  secondary:
    type: openai
    base_url: %[2]q
    timeout: %[4]dms
  anthro:
    type: anthropic
    base_url: %[3]q
    timeout: %[4]dms
models:
  chat-group:
    strategy: priority
    deployments:
      - {provider: primary, model: mock-small, priority: 1}
      - {provider: secondary, model: mock-small, priority: 2}
  claude-group:
    strategy: priority
    deployments:
      - {provider: anthro, model: mock-small, priority: 1}
`

// newStack builds the full real chain for one subtest and returns an
// httptest.Server fronting it, with its own router/breaker state and
// Prometheus registry so scenarios never leak state into each other.
func newStack(t *testing.T, sc stackConfig) *testStack {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/config.yaml"
	// base_url carries the version path; adapters append only "/chat/completions" or "/messages"
	body := fmt.Sprintf(cfgTemplate, sc.PrimaryURL+"/v1", sc.SecondaryURL+"/v1", sc.AnthropicURL+"/v1", sc.ProviderTimeMs, sc.CooldownMs)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := config.Load(path, func(string) (string, bool) { return "", false })
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	rt, err := buildRouter(cfg, m, log)
	if err != nil {
		t.Fatalf("buildRouter: %v", err)
	}
	gw := gateway.New(rt, nil, gateway.NewMemorySessions(cfg.Router.SessionTTL), m, log)
	srv := api.NewServer(api.Deps{
		Log: log, Gateway: gw, Metrics: m, MaxBodyBytes: cfg.Server.MaxBodyBytes,
		RequestTimeout: cfg.Server.RequestTimeout, CacheOn: false,
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return &testStack{ts: ts, gw: gw}
}

func chatBody(model string) string {
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hi"}]}`, model)
}

func (st *testStack) post(t *testing.T, path, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(st.ts.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	return resp
}

func decodeJSON[T any](t *testing.T, r io.Reader) T {
	t.Helper()
	var v T
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	return v
}

type chatCompletion struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

func TestE2E_HappyPath(t *testing.T) {
	t.Parallel()
	primaryURL, _ := newMock(t, "primary")
	secondaryURL, _ := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 200})

	resp := st.post(t, "/v1/chat/completions", chatBody("chat-group"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get(api.HeaderProvider); got != "primary/mock-small" {
		t.Errorf("provider header = %q, want %q", got, "primary/mock-small")
	}
	if got := resp.Header.Get(api.HeaderAttempts); got != "1" {
		t.Errorf("attempts header = %q, want %q", got, "1")
	}
	cc := decodeJSON[chatCompletion](t, resp.Body)
	if len(cc.Choices) != 1 || !strings.Contains(cc.Choices[0].Message.Content, "[primary]") {
		t.Errorf("unexpected response body: %+v", cc)
	}
}

func TestE2E_FailoverThenBreakerOpens(t *testing.T) {
	t.Parallel()
	primaryURL, setPrimary := newMock(t, "primary")
	secondaryURL, _ := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 300})

	// primary fails every request from now on; secondary is untouched
	setPrimary(t, `{"error_rate":1,"error_status":503}`)

	// 1st consecutive failure: fails over to secondary, client still gets 200
	resp := st.post(t, "/v1/chat/completions", chatBody("chat-group"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get(api.HeaderProvider); got != "secondary/mock-small" {
		t.Errorf("provider header = %q, want secondary/mock-small", got)
	}
	if got := resp.Header.Get(api.HeaderAttempts); got != "2" {
		t.Errorf("attempts header = %q, want 2", got)
	}
	resp.Body.Close()

	// 2nd consecutive failure trips the breaker to open; still fails over to secondary
	resp = st.post(t, "/v1/chat/completions", chatBody("chat-group"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get(api.HeaderAttempts); got != "2" {
		t.Errorf("attempts header = %q, want 2", got)
	}
	resp.Body.Close()

	if state := st.breakerState("primary/mock-small"); state != "open" {
		t.Fatalf("primary breaker state = %q, want open", state)
	}

	// breaker now denies the attempt outright; only secondary is tried, Attempts=1
	resp = st.post(t, "/v1/chat/completions", chatBody("chat-group"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get(api.HeaderProvider); got != "secondary/mock-small" {
		t.Errorf("provider header = %q, want secondary/mock-small", got)
	}
	if got := resp.Header.Get(api.HeaderAttempts); got != "1" {
		t.Errorf("attempts header = %q, want 1 (breaker-skipped primary)", got)
	}
	resp.Body.Close()
}

func TestE2E_HungPrimaryFailsOverWithinTimeout(t *testing.T) {
	t.Parallel()
	primaryURL, setPrimary := newMock(t, "primary")
	secondaryURL, _ := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	const providerTimeoutMs = 200
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: providerTimeoutMs, CooldownMs: 1000})

	setPrimary(t, `{"fault":"hang"}`)

	start := time.Now()
	resp := st.post(t, "/v1/chat/completions", chatBody("chat-group"))
	elapsed := time.Since(start)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get(api.HeaderProvider); got != "secondary/mock-small" {
		t.Errorf("provider header = %q, want secondary/mock-small", got)
	}
	// 10x margin on the provider timeout absorbs CI scheduling jitter
	if max := 10 * providerTimeoutMs * time.Millisecond; elapsed > max {
		t.Errorf("request took %v, want well under %v (bounded by provider timeout)", elapsed, max)
	}
}

func TestE2E_BothFail502(t *testing.T) {
	t.Parallel()
	primaryURL, setPrimary := newMock(t, "primary")
	secondaryURL, setSecondary := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 1000})

	setPrimary(t, `{"error_rate":1,"error_status":503}`)
	setSecondary(t, `{"error_rate":1,"error_status":503}`)

	resp := st.post(t, "/v1/chat/completions", chatBody("chat-group"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502, body = %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get(api.HeaderAttempts); got == "" {
		t.Errorf("attempts header missing")
	}
	if got := resp.Header.Get(api.HeaderProvider); got == "" {
		t.Errorf("provider header missing")
	}
}

func TestE2E_Both404DoesNotTripBreaker(t *testing.T) {
	t.Parallel()
	primaryURL, setPrimary := newMock(t, "primary")
	secondaryURL, setSecondary := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 1000})

	setPrimary(t, `{"error_rate":1,"error_status":404}`)
	setSecondary(t, `{"error_rate":1,"error_status":404}`)

	resp := st.post(t, "/v1/chat/completions", chatBody("chat-group"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadGateway {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 502, body = %s", resp.StatusCode, b)
	}
	for _, id := range []string{"primary/mock-small", "secondary/mock-small"} {
		if state := st.breakerState(id); state != "closed" {
			t.Errorf("breaker %s state = %q, want closed (404 must not count as a provider failure)", id, state)
		}
	}
}

// readSSE drains an SSE body into (data lines, done-seen, error-event-seen).
func readSSE(t *testing.T, body io.Reader) (events []string, sawDone bool, sawErrorEvent bool) {
	t.Helper()
	sc := bufio.NewScanner(body)
	for sc.Scan() {
		line := sc.Text()
		data, ok := strings.CutPrefix(line, "data: ")
		if !ok {
			continue
		}
		if data == "[DONE]" {
			sawDone = true
			continue
		}
		events = append(events, data)
		if strings.Contains(data, `"error"`) {
			sawErrorEvent = true
		}
	}
	return events, sawDone, sawErrorEvent
}

func streamBody(model string) string {
	return fmt.Sprintf(`{"model":%q,"stream":true,"messages":[{"role":"user","content":"hi"}]}`, model)
}

func TestE2E_StreamingSilentFailover(t *testing.T) {
	t.Parallel()
	primaryURL, setPrimary := newMock(t, "primary")
	secondaryURL, _ := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 1000})

	// fails before the first token, so ChatStream can still fail over silently
	setPrimary(t, `{"error_rate":1,"error_status":503}`)

	resp := st.post(t, "/v1/chat/completions", streamBody("chat-group"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get(api.HeaderProvider); got != "secondary/mock-small" {
		t.Errorf("provider header = %q, want secondary/mock-small", got)
	}
	if got := resp.Header.Get(api.HeaderAttempts); got != "2" {
		t.Errorf("attempts header = %q, want 2", got)
	}
	events, sawDone, sawErr := readSSE(t, resp.Body)
	if len(events) == 0 {
		t.Fatalf("no SSE chunks received")
	}
	if sawErr {
		t.Errorf("unexpected error event in a silently-failed-over stream")
	}
	if !sawDone {
		t.Errorf("stream ended without [DONE]")
	}
}

func TestE2E_StreamingMidStreamFailure(t *testing.T) {
	t.Parallel()
	primaryURL, setPrimary := newMock(t, "primary")
	secondaryURL, _ := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 1000})

	// first token lands, then dies: once the client has bytes the gateway can't
	// switch providers, it must emit an in-band error and skip [DONE]
	setPrimary(t, `{"fail_after_tokens":3,"tokens_per_sec":500}`)

	resp := st.post(t, "/v1/chat/completions", streamBody("chat-group"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get(api.HeaderProvider); got != "primary/mock-small" {
		t.Errorf("provider header = %q, want primary/mock-small", got)
	}
	events, sawDone, sawErr := readSSE(t, resp.Body)
	if len(events) == 0 {
		t.Fatalf("no SSE chunks received before the mid-stream failure")
	}
	if !sawErr {
		t.Errorf("expected an in-band error event after the mid-stream failure")
	}
	if sawDone {
		t.Errorf("stream must not end with [DONE] after a mid-stream failure")
	}
}

func TestE2E_AnthropicAdapterReturnsOpenAIShape(t *testing.T) {
	t.Parallel()
	primaryURL, _ := newMock(t, "primary")
	secondaryURL, _ := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 200})

	resp := st.post(t, "/v1/chat/completions", chatBody("claude-group"))
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body = %s", resp.StatusCode, b)
	}
	if got := resp.Header.Get(api.HeaderProvider); got != "anthro/mock-small" {
		t.Errorf("provider header = %q, want anthro/mock-small", got)
	}
	cc := decodeJSON[chatCompletion](t, resp.Body)
	if len(cc.Choices) != 1 || !strings.Contains(cc.Choices[0].Message.Content, "[anthro]") {
		t.Errorf("unexpected response body (should be OpenAI-shaped regardless of upstream wire format): %+v", cc)
	}
}

func TestE2E_UnsupportedParameterRejectedBeforeCallingProviders(t *testing.T) {
	t.Parallel()
	primaryURL, setPrimary := newMock(t, "primary")
	secondaryURL, setSecondary := newMock(t, "secondary")
	anthroURL, _ := newMock(t, "anthro")
	st := newStack(t, stackConfig{PrimaryURL: primaryURL, SecondaryURL: secondaryURL, AnthropicURL: anthroURL, ProviderTimeMs: 2000, CooldownMs: 200})

	// tripwire: a wrongly-forwarded request comes back 503 instead of the 400 we expect
	setPrimary(t, `{"error_rate":1,"error_status":503}`)
	setSecondary(t, `{"error_rate":1,"error_status":503}`)

	body := `{"model":"chat-group","seed":42,"messages":[{"role":"user","content":"hi"}]}`
	resp := st.post(t, "/v1/chat/completions", body)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 400, body = %s", resp.StatusCode, b)
	}
	eb := decodeJSON[struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}](t, resp.Body)
	if !strings.Contains(eb.Error.Message, "seed") {
		t.Errorf("error message = %q, want it to name the unsupported parameter", eb.Error.Message)
	}
	if state := st.breakerState("primary/mock-small"); state != "closed" {
		t.Errorf("primary breaker state = %q, want closed: provider must never have been called", state)
	}
	if state := st.breakerState("secondary/mock-small"); state != "closed" {
		t.Errorf("secondary breaker state = %q, want closed: provider must never have been called", state)
	}
}
