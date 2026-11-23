// Package provider defines the interface every LLM adapter implements, plus the
// pieces all adapters share: a tuned HTTP client, per-attempt deadlines, error
// classification and an SSE reader. Concrete adapters live in subpackages.
package provider

import (
	"context"
	"net/http"
	"time"

	"llmgateway/internal/llm"
)

type Provider interface {
	Name() string
	Chat(ctx context.Context, model string, req *llm.ChatRequest) (*llm.ChatResponse, error)
	ChatStream(ctx context.Context, model string, req *llm.ChatRequest) (Stream, error)
}

// Stream is pull-based (like a gRPC client stream) rather than a channel:
// no extra goroutine per stream, nothing to leak when the client disconnects,
// and backpressure is natural: we only read from upstream as fast as we can
// write to the client.
//
// Recv returns only chunks that carry something (text, finish reason or usage),
// so the first successful Recv means "the first real token arrived".
type Stream interface {
	Recv() (llm.StreamChunk, error) // io.EOF on clean end
	Close() error
}

// NewHTTPClient returns the single client shared by all adapters.
//
// The default transport keeps only 2 idle connections per host. At thousands of
// requests per second almost every call would open a fresh TCP (+TLS)
// connection and the box would run out of ephemeral ports in TIME_WAIT. Raising
// MaxIdleConnsPerHost is the most important line for throughput here.
//
// No http.Client.Timeout: it caps the whole body read and would kill long
// streams. Timeouts are applied per attempt via Deadline instead.
func NewHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 0 // no global cap
	t.MaxIdleConnsPerHost = 1024
	t.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: t}
}
