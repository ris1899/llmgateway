package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func newTestServer() (*Server, *bytes.Buffer) {
	var buf bytes.Buffer
	return NewServer(Deps{Log: slog.New(slog.NewJSONHandler(&buf, nil)), MaxBodyBytes: 1 << 20}), &buf
}

func TestHealthAndReadiness(t *testing.T) {
	s, _ := newTestServer()
	h := s.Handler()

	get := func(path string) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		return rec.Code
	}

	if got := get("/healthz"); got != http.StatusOK {
		t.Errorf("healthz = %d, want 200", got)
	}
	if got := get("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("readyz before SetReady = %d, want 503", got)
	}
	s.SetReady(true)
	if got := get("/readyz"); got != http.StatusOK {
		t.Errorf("readyz after SetReady = %d, want 200", got)
	}
	s.SetReady(false) // shutdown begins
	if got := get("/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("readyz during shutdown = %d, want 503", got)
	}
}

func TestMethodNotAllowed(t *testing.T) {
	s, _ := newTestServer()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/healthz", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d, want 405", rec.Code)
	}
}

func TestRequestID(t *testing.T) {
	s, _ := newTestServer()
	h := s.Handler()

	tests := []struct {
		name, in string
		keep     bool
	}{
		{"generated when absent", "", false},
		{"caller id propagated", "abc-123_X", true},
		{"unsafe chars replaced", "abc\ninjected", false},
		{"oversized replaced", strings.Repeat("a", 65), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
			if tt.in != "" {
				req.Header.Set(HeaderRequestID, tt.in)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			got := rec.Header().Get(HeaderRequestID)
			if tt.keep && got != tt.in {
				t.Errorf("id = %q, want %q", got, tt.in)
			}
			if !tt.keep && (got == tt.in || len(got) != 32) {
				t.Errorf("id = %q, want fresh 32-char id", got)
			}
		})
	}
}

func TestPanicRecoveryAndAccessLog(t *testing.T) {
	s, logs := newTestServer()
	s.mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("kaboom") })

	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	var body errorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body.Error.Type != "server_error" {
		t.Errorf("body not OpenAI-shaped: %s", rec.Body)
	}
	out := logs.String()
	if !strings.Contains(out, `"panic":"kaboom"`) || !strings.Contains(out, `"status":500`) {
		t.Errorf("expected panic + access log lines, got:\n%s", out)
	}
}

// Guards the Unwrap method on statusRecorder: if it is removed, Flush through
// the middleware chain fails and SSE streaming would silently buffer.
func TestFlushThroughMiddleware(t *testing.T) {
	s, _ := newTestServer()
	flushErr := make(chan error, 1)
	s.mux.HandleFunc("GET /flush", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("x"))
		flushErr <- http.NewResponseController(w).Flush()
	})
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/flush")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err := <-flushErr; err != nil {
		t.Errorf("flush through middleware failed: %v", err)
	}
}
