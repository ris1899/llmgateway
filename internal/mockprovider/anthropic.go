package mockprovider

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Anthropic Messages API wire format, only the fields the mock needs.

type anRequest struct {
	Model     string          `json:"model"`
	MaxTokens int             `json:"max_tokens"`
	System    json.RawMessage `json:"system"`
	Messages  []struct {
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Stream bool `json:"stream"`
}

type anUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

func writeAnthropicError(w http.ResponseWriter, status int, msg string) {
	typ := "api_error"
	switch status {
	case http.StatusTooManyRequests:
		typ = "rate_limit_error"
	case 529:
		typ = "overloaded_error"
	case http.StatusBadRequest:
		typ = "invalid_request_error"
	}
	writeJSON(w, status, map[string]any{"type": "error", "error": map[string]string{"type": typ, "message": msg}})
}

func (s *Server) handleAnthropic(w http.ResponseWriter, r *http.Request) {
	b := s.behavior.Load()
	var req anRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAnthropicError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	// Real Anthropic rejects requests without max_tokens. Enforcing it here
	// proves the adapter always sets it.
	if req.MaxTokens <= 0 {
		writeAnthropicError(w, http.StatusBadRequest, "max_tokens: field required")
		return
	}
	if !s.prelude(w, r, b, writeAnthropicError) {
		return
	}

	in := estimateTokens(len(req.System))
	for _, m := range req.Messages {
		in += estimateTokens(len(m.Content))
	}
	toks, truncated := s.reply(b, req.MaxTokens)
	stop := "end_turn"
	if truncated {
		stop = "max_tokens"
	}
	id := s.nextID("msg_")

	if !req.Stream {
		writeJSON(w, http.StatusOK, map[string]any{
			"id": id, "type": "message", "role": "assistant", "model": req.Model,
			"content":     []map[string]string{{"type": "text", "text": strings.Join(toks, "")}},
			"stop_reason": stop, "stop_sequence": nil,
			"usage": anUsage{InputTokens: in, OutputTokens: len(toks)},
		})
		return
	}

	// Anthropic streams named events; text arrives in content_block_delta and
	// the final output token count in message_delta.
	sseHeaders(w)
	rc := http.NewResponseController(w)
	_ = writeSSE(w, "message_start", map[string]any{"type": "message_start", "message": map[string]any{
		"id": id, "type": "message", "role": "assistant", "model": req.Model, "content": []any{},
		"stop_reason": nil, "usage": anUsage{InputTokens: in, OutputTokens: 1},
	}})
	_ = writeSSE(w, "content_block_start", map[string]any{"type": "content_block_start", "index": 0,
		"content_block": map[string]string{"type": "text", "text": ""}})
	ok := streamTokens(r.Context(), rc, b, toks, func(_ int, tok string) error {
		return writeSSE(w, "content_block_delta", map[string]any{"type": "content_block_delta", "index": 0,
			"delta": map[string]string{"type": "text_delta", "text": tok}})
	})
	if !ok {
		return
	}
	_ = writeSSE(w, "content_block_stop", map[string]any{"type": "content_block_stop", "index": 0})
	_ = writeSSE(w, "message_delta", map[string]any{"type": "message_delta",
		"delta": map[string]any{"stop_reason": stop, "stop_sequence": nil},
		"usage": map[string]int{"output_tokens": len(toks)}})
	_ = writeSSE(w, "message_stop", map[string]string{"type": "message_stop"})
	_ = rc.Flush()
}
