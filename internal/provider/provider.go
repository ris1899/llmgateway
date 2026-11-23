// Package provider defines the LLM adapter interface and shared HTTP/SSE/error-classification plumbing.
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

// Stream is pull-based: no extra goroutine, natural backpressure. Close
// must always be called (idempotent) to release the Deadline and body.
type Stream interface {
	Recv() (llm.StreamChunk, error) // io.EOF on clean end
	Close() error
}

// NewHTTPClient raises MaxIdleConnsPerHost from the default 2 to avoid
// TCP/TLS churn at high rps. No Timeout: it would cap long streams.
func NewHTTPClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 0 // no global cap
	t.MaxIdleConnsPerHost = 1024
	t.IdleConnTimeout = 90 * time.Second
	return &http.Client{Transport: t}
}
