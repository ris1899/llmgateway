// Package mockprovider is a fake LLM server whose behavior (latency, errors,
// faults, streaming speed) is set at startup and can be changed at runtime via
// an admin endpoint. It speaks both the OpenAI and the Anthropic wire formats,
// so every adapter can be exercised end to end with zero API keys and zero cost.
package mockprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Duration marshals as "150ms" instead of nanoseconds so the admin API is
// usable from curl.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

type Behavior struct {
	Latency     Duration `json:"latency"`      // before the response (non-stream) or the first token (stream)
	Jitter      Duration `json:"jitter"`       // extra uniform random latency in [0, jitter)
	ErrorRate   float64  `json:"error_rate"`   // probability [0,1] of answering with ErrorStatus
	ErrorStatus int      `json:"error_status"` // e.g. 500, 503, 429
	// Fault applies to every request: "hang" never answers (exercises timeouts),
	// "reset" kills the TCP connection (exercises transport errors). "" = off.
	Fault           string  `json:"fault"`
	OutputTokens    int     `json:"output_tokens"`
	TokensPerSec    float64 `json:"tokens_per_sec"`    // streaming pace; 0 = as fast as possible
	FailAfterTokens int     `json:"fail_after_tokens"` // >0: abort the stream after N tokens
}

func DefaultBehavior() Behavior {
	return Behavior{
		Latency:      Duration(50 * time.Millisecond),
		ErrorStatus:  http.StatusInternalServerError,
		OutputTokens: 20,
		TokensPerSec: 50,
	}
}

func (b Behavior) validate() error {
	switch {
	case b.ErrorRate < 0 || b.ErrorRate > 1:
		return errors.New("error_rate must be in [0,1]")
	case b.ErrorStatus < 400 || b.ErrorStatus > 599:
		return errors.New("error_status must be 4xx or 5xx")
	case b.Fault != "" && b.Fault != "hang" && b.Fault != "reset":
		return errors.New(`fault must be "", "hang" or "reset"`)
	case b.OutputTokens < 1:
		return errors.New("output_tokens must be >= 1")
	case b.TokensPerSec < 0 || b.Latency < 0 || b.Jitter < 0:
		return errors.New("latency, jitter and tokens_per_sec must be >= 0")
	}
	return nil
}

type Server struct {
	name     string
	behavior atomic.Pointer[Behavior] // swapped atomically: hot path reads never lock
	requests atomic.Int64
	mux      *http.ServeMux
}

func New(name string, b Behavior) (*Server, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	s := &Server{name: name, mux: http.NewServeMux()}
	s.behavior.Store(&b)
	s.mux.HandleFunc("POST /v1/chat/completions", s.handleOpenAI)
	s.mux.HandleFunc("POST /v1/messages", s.handleAnthropic)
	s.mux.HandleFunc("GET /v1/models", s.handleModels)
	s.mux.HandleFunc("GET /admin/behavior", s.handleGetBehavior)
	s.mux.HandleFunc("PATCH /admin/behavior", s.handlePatchBehavior)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	return s, nil
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) handleGetBehavior(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.behavior.Load())
}

// PATCH merges: decoding into a copy of the current behavior only overwrites
// the fields present in the body, so `{"error_rate":1}` changes just that.
func (s *Server) handlePatchBehavior(w http.ResponseWriter, r *http.Request) {
	b := *s.behavior.Load()
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := b.validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.behavior.Store(&b)
	writeJSON(w, http.StatusOK, &b)
}

// prelude applies faults, latency and injected errors; returns false once the
// request is already answered or abandoned.
func (s *Server) prelude(w http.ResponseWriter, r *http.Request, b *Behavior, writeErr func(http.ResponseWriter, int, string)) bool {
	s.requests.Add(1)
	switch b.Fault {
	case "hang":
		<-r.Context().Done()
		return false
	case "reset":
		resetConn(w)
		return false
	}
	d := time.Duration(b.Latency)
	if b.Jitter > 0 {
		d += rand.N(time.Duration(b.Jitter))
	}
	if !sleep(r.Context(), d) {
		return false
	}
	if b.ErrorRate > 0 && rand.Float64() < b.ErrorRate {
		if b.ErrorStatus == http.StatusTooManyRequests {
			w.Header().Set("Retry-After", "1")
		}
		writeErr(w, b.ErrorStatus, "injected failure")
		return false
	}
	return true
}

// resetConn closes the TCP connection with SO_LINGER=0, which sends an RST
// instead of a graceful FIN: the client sees "connection reset by peer", the
// same thing it sees when a provider's load balancer drops it.
func resetConn(w http.ResponseWriter) {
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	_ = conn.Close()
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

var words = strings.Fields("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua")

// reply builds the output tokens. The first token is the mock's name, so the
// response body shows which provider actually answered (useful in demos).
func (s *Server) reply(b *Behavior, maxTokens int) ([]string, bool) {
	n, truncated := b.OutputTokens, false
	if maxTokens > 0 && maxTokens < n {
		n, truncated = maxTokens, true
	}
	toks := make([]string, n)
	toks[0] = "[" + s.name + "]"
	for i := 1; i < n; i++ {
		toks[i] = " " + words[i%len(words)]
	}
	return toks, truncated
}

// streamTokens paces tokens and emits each one. Returns false if the client
// went away. FailAfterTokens aborts the connection mid-stream.
func streamTokens(ctx context.Context, rc *http.ResponseController, b *Behavior, toks []string, emit func(i int, tok string) error) bool {
	var interval time.Duration
	if b.TokensPerSec > 0 {
		interval = time.Duration(float64(time.Second) / b.TokensPerSec)
	}
	for i, tok := range toks {
		if i > 0 && !sleep(ctx, interval) {
			return false
		}
		if b.FailAfterTokens > 0 && i == b.FailAfterTokens {
			// Abort without the terminating chunk: the client sees an unexpected
			// EOF, exactly like a provider crashing mid-stream.
			panic(http.ErrAbortHandler)
		}
		if emit(i, tok) != nil || rc.Flush() != nil {
			return false
		}
	}
	return true
}

// Rough token estimate (~4 bytes per token). Good enough for a mock.
func estimateTokens(n int) int { return n/4 + 1 }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeSSE(w http.ResponseWriter, event string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if event != "" {
		_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, b)
	} else {
		_, err = fmt.Fprintf(w, "data: %s\n\n", b)
	}
	return err
}

func sseHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) nextID(prefix string) string {
	return prefix + s.name + "-" + strconv.FormatInt(s.requests.Load(), 10)
}

func (s *Server) handleModels(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data":   []map[string]any{{"id": "mock-small", "object": "model", "owned_by": s.name}},
	})
}
