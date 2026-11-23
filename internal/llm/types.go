// Package llm holds the gateway's canonical request/response types.
package llm

// Follows OpenAI naming; adapters map to/from provider-specific shapes.
const (
	RoleSystem    = "system"
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

// Message is text-only in v1; tools and images are rejected at the API edge.
type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ChatRequest is the normalized request; sampling params are pointers so
// unset is distinguishable from 0.
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
	InputTokens  int  `json:"input_tokens"`
	OutputTokens int  `json:"output_tokens"`
	Estimated    bool `json:"estimated,omitempty"` // true if we counted tokens ourselves
}

type ChatResponse struct {
	ID           string `json:"id"`
	Model        string `json:"model"` // upstream model that actually served it
	Content      string `json:"content"`
	FinishReason string `json:"finish_reason"`
	Usage        Usage  `json:"usage"`
}

// StreamChunk is one streaming delta; Usage is usually only set on the last.
type StreamChunk struct {
	Delta        string
	FinishReason string
	Usage        *Usage
}
