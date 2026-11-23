package provider

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"llmgateway/internal/llm"
)

func TestStatusError_KindByStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status     int
		retryAfter string // header value, "" = none
		wantKind   llm.ErrorKind
		wantRetry  time.Duration
	}{
		{status: 400, wantKind: llm.KindBadRequest},
		{status: 413, wantKind: llm.KindBadRequest},
		{status: 422, wantKind: llm.KindBadRequest},
		{status: 401, wantKind: llm.KindAuth},
		{status: 403, wantKind: llm.KindAuth},
		{status: 404, wantKind: llm.KindNotFound},
		{status: 408, wantKind: llm.KindTimeout},
		{status: 429, retryAfter: "5", wantKind: llm.KindRateLimited, wantRetry: 5 * time.Second},
		{status: 500, wantKind: llm.KindUnavailable},
		{status: 503, wantKind: llm.KindUnavailable},
		{status: 529, wantKind: llm.KindUnavailable}, // Anthropic "overloaded"
		{status: 418, wantKind: llm.KindUnknown},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%d", tt.status), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.retryAfter != "" {
					w.Header().Set("Retry-After", tt.retryAfter)
				}
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte("boom"))
			}))
			defer srv.Close()

			resp, err := http.Get(srv.URL) //nolint:noctx // test-only, no cancellation needed
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			e := StatusError("p", resp)
			_ = resp.Body.Close()
			if e.Kind != tt.wantKind {
				t.Errorf("Kind = %v, want %v", e.Kind, tt.wantKind)
			}
			if e.Status != tt.status {
				t.Errorf("Status = %d, want %d", e.Status, tt.status)
			}
			if tt.wantRetry != 0 && e.RetryAfter != tt.wantRetry {
				t.Errorf("RetryAfter = %v, want %v", e.RetryAfter, tt.wantRetry)
			}
		})
	}
}

func TestParseRetryAfter_HTTPDate(t *testing.T) {
	t.Parallel()
	future := time.Now().Add(10 * time.Second).UTC()
	got := parseRetryAfter(future.Format(http.TimeFormat))
	// Allow a couple seconds of slack for formatting/rounding to the second.
	if got < 7*time.Second || got > 11*time.Second {
		t.Errorf("parseRetryAfter(date) = %v, want ~10s", got)
	}
}

func TestParseRetryAfter_Invalid(t *testing.T) {
	t.Parallel()
	if got := parseRetryAfter("not-a-value"); got != 0 {
		t.Errorf("parseRetryAfter(invalid) = %v, want 0", got)
	}
}

// A large error body must not prevent the connection going back to the idle
// pool: StatusError only reads the first 4 KiB but must drain the rest.
func TestStatusError_DrainsBodyForConnectionReuse(t *testing.T) {
	t.Parallel()

	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(strings.Repeat("e", 32<<10))) // far more than the 4 KiB read
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	const requests = 5
	hc := &http.Client{}
	for i := 0; i < requests; i++ {
		resp, err := hc.Get(srv.URL) //nolint:noctx
		if err != nil {
			t.Fatalf("GET %d: %v", i, err)
		}
		_ = StatusError("p", resp)
		_ = resp.Body.Close()
	}

	// Allow a little slack for an occasional reconnect rather than asserting
	// an exact connection count.
	if got := conns.Load(); int(got) >= requests {
		t.Errorf("new connections opened = %d for %d requests, want far fewer (connections should mostly be reused)", got, requests)
	}
}

func TestTransportError_Classification(t *testing.T) {
	t.Parallel()

	t.Run("deadline fired", func(t *testing.T) {
		t.Parallel()
		dl := NewDeadline(context.Background(), time.Millisecond)
		defer dl.Release()
		waitForFired(t, dl, true)
		e := TransportError("p", errors.New("read: connection reset"), dl)
		if e.Kind != llm.KindTimeout {
			t.Errorf("Kind = %v, want KindTimeout", e.Kind)
		}
	})

	t.Run("parent canceled", func(t *testing.T) {
		t.Parallel()
		parent, cancel := context.WithCancel(context.Background())
		dl := NewDeadline(parent, time.Second)
		defer dl.Release()
		cancel()
		e := TransportError("p", errors.New("read: connection reset"), dl)
		if e.Kind != llm.KindCanceled {
			t.Errorf("Kind = %v, want KindCanceled", e.Kind)
		}
	})

	t.Run("parent canceled wins over a same-time deadline fire", func(t *testing.T) {
		t.Parallel()
		// Client cancellation and the no-progress deadline land at the same
		// time; the client leaving must win.
		parent, cancel := context.WithCancel(context.Background())
		dl := NewDeadline(parent, time.Millisecond)
		defer dl.Release()
		waitForFired(t, dl, true)
		cancel()
		e := TransportError("p", errors.New("read: connection reset"), dl)
		if e.Kind != llm.KindCanceled {
			t.Errorf("Kind = %v, want KindCanceled", e.Kind)
		}
	})

	t.Run("parent deadline exceeded", func(t *testing.T) {
		t.Parallel()
		parent, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		<-parent.Done()
		dl := NewDeadline(parent, time.Second)
		defer dl.Release()
		e := TransportError("p", errors.New("read: connection reset"), dl)
		if e.Kind != llm.KindTimeout {
			t.Errorf("Kind = %v, want KindTimeout", e.Kind)
		}
	})

	t.Run("already-classified error keeps its kind even if the deadline fired", func(t *testing.T) {
		t.Parallel()
		// E.g. the SSE reader's size limit; a fired deadline must not relabel it.
		dl := NewDeadline(context.Background(), time.Millisecond)
		defer dl.Release()
		waitForFired(t, dl, true)
		inner := &llm.Error{Kind: llm.KindUnavailable, Err: errors.New("sse: line too long")}
		e := TransportError("p", fmt.Errorf("reading stream: %w", inner), dl)
		if e.Kind != llm.KindUnavailable {
			t.Errorf("Kind = %v, want KindUnavailable (kept from the classified error)", e.Kind)
		}
		if e.Provider != "p" {
			t.Errorf("Provider = %q, want %q", e.Provider, "p")
		}
	})

	t.Run("plain net error, no cancellation", func(t *testing.T) {
		t.Parallel()
		dl := NewDeadline(context.Background(), time.Second)
		defer dl.Release()
		e := TransportError("p", errors.New("dial tcp: connection refused"), dl)
		if e.Kind != llm.KindUnavailable {
			t.Errorf("Kind = %v, want KindUnavailable", e.Kind)
		}
	})
}

// Build() running after the caller's context is already canceled must
// report Canceled, not BadRequest.
func TestDo_BuildFailsWithCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := Do(&http.Client{}, "p", time.Second, ctx, func(ctx context.Context) (*http.Request, error) { //nolint:bodyclose // build fails, Do returns a nil response.
		// A build step that itself checks ctx, like context.Cause or a
		// cancellation-aware marshal step, fails here because ctx is done.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return http.NewRequestWithContext(ctx, http.MethodGet, "http://example.invalid", nil)
	})
	e := llm.AsError(err)
	if e.Kind != llm.KindCanceled {
		t.Errorf("Kind = %v, want KindCanceled", e.Kind)
	}
}

// TestDo_BuildFailsForOtherReason verifies build() failures unrelated to
// context cancellation are still reported as BadRequest.
func TestDo_BuildFailsForOtherReason(t *testing.T) {
	t.Parallel()
	_, _, err := Do(&http.Client{}, "p", time.Second, context.Background(), func(ctx context.Context) (*http.Request, error) { //nolint:bodyclose // build fails, Do returns a nil response.
		return nil, errors.New("bad url")
	})
	e := llm.AsError(err)
	if e.Kind != llm.KindBadRequest {
		t.Errorf("Kind = %v, want KindBadRequest", e.Kind)
	}
}
