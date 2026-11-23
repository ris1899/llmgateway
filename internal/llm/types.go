// Package llm holds the gateway's canonical request/response types.
//
// Every provider adapter translates to and from these types, so the rest of the
// gateway (router, cache, usage) never sees provider-specific shapes. This
// package must not import any other internal package: it is the leaf that
// breaks what would otherwise be import cycles between api, router and provider.
package llm

// Role values follow OpenAI naming because our public API is OpenAI-compatible.
// Adapters map them (e.g. Anthropic pulls "system" out into a separate field).
const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

// Message is text-only in v1. Tools and images are rejected at the API edge
// with an explicit 400 rather than silently dropped (see DECISIONS.md).
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the normalized request.
//
// Optional sampling params are pointers so "unset" is distinguishable from 0.
// That matters for caching: temperature=0 is deterministic-ish and cacheable,
// while unset means the provider default (1.0 for OpenAI) and is not.
type ChatRequest struct {
	Model       string    `json:"model"`
	Messages    []Message `json:"messages"`
	Temperature *float64  `json:"temperature,omitempty"`
	TopP        *float64  `json:"top_p,omitempty"`
	MaxTokens   *int      `json:"max_tokens,omitempty"`
	Stop        []string  `json:"stop,omitempty"`
	Stream      bool      `json:"stream,omitempty"`
}

type Usage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	// Estimated is true when the provider did not report usage (e.g. client
	// disconnected mid-stream) and we counted tokens ourselves. Cost reports
	// carry this flag so estimated spend is never presented as billed spend.
	Estimated bool `json:"estimated,omitempty"`
}

type ChatResponse struct {
	ID           string `json:"id"`
	Model        string `json:"model"` // upstream model that actually served it
	Content      string `json:"content"`
	FinishReason string `json:"finish_reason"`
	Usage        Usage  `json:"usage"`
}

// StreamChunk is one normalized streaming delta. Usage is usually only set on
// the final chunk, and only if the provider reports it.
type StreamChunk struct {
	Delta        string
	FinishReason string
	Usage        *Usage
}
