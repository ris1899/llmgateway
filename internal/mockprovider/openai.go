package mockprovider

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// OpenAI Chat Completions wire format, only the fields the mock needs.

type oaRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	MaxTokens     int  `json:"max_tokens"`
	Stream        bool `json:"stream"`
	StreamOptions *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`
}

type oaUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type oaMessage struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

type oaChoice struct {
	Index        int        `json:"index"`
	Message      *oaMessage `json:"message,omitempty"`
	Delta        *oaMessage `json:"delta,omitempty"`
	FinishReason *string    `json:"finish_reason"`
}

type oaResponse struct {
	ID      string     `json:"id"`
	Object  string     `json:"object"`
	Created int64      `json:"created"`
	Model   string     `json:"model"`
	Choices []oaChoice `json:"choices"`
	Usage   *oaUsage   `json:"usage,omitempty"`
}

func writeOpenAIError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"message": msg, "type": "mock_error"}})
}

func (s *Server) handleOpenAI(w http.ResponseWriter, r *http.Request) {
	b := s.behavior.Load()
	var req oaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeOpenAIError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if !s.prelude(w, r, b, writeOpenAIError) {
		return
	}

	in := 0
	for _, m := range req.Messages {
		in += estimateTokens(len(m.Content))
	}
	toks, truncated := s.reply(b, req.MaxTokens)
	finish := "stop"
	if truncated {
		finish = "length"
	}
	usage := &oaUsage{PromptTokens: in, CompletionTokens: len(toks), TotalTokens: in + len(toks)}
	id, now := s.nextID("chatcmpl-"), time.Now().Unix()

	if !req.Stream {
		writeJSON(w, http.StatusOK, oaResponse{
			ID: id, Object: "chat.completion", Created: now, Model: req.Model,
			Choices: []oaChoice{{Message: &oaMessage{Role: "assistant", Content: strings.Join(toks, "")}, FinishReason: &finish}},
			Usage:   usage,
		})
		return
	}

	sseHeaders(w)
	rc := http.NewResponseController(w)
	chunk := func(c oaChoice, u *oaUsage) oaResponse {
		choices := []oaChoice{c}
		if u != nil {
			choices = []oaChoice{} // OpenAI's usage chunk has an empty choices array
		}
		return oaResponse{ID: id, Object: "chat.completion.chunk", Created: now, Model: req.Model, Choices: choices, Usage: u}
	}
	ok := streamTokens(r.Context(), rc, b, toks, func(i int, tok string) error {
		d := &oaMessage{Content: tok}
		if i == 0 {
			d.Role = "assistant"
		}
		return writeSSE(w, "", chunk(oaChoice{Delta: d}, nil))
	})
	if !ok {
		return
	}
	_ = writeSSE(w, "", chunk(oaChoice{Delta: &oaMessage{}, FinishReason: &finish}, nil))
	if req.StreamOptions != nil && req.StreamOptions.IncludeUsage {
		_ = writeSSE(w, "", chunk(oaChoice{}, usage))
	}
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
	_ = rc.Flush()
}
