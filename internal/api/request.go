package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"llmgateway/internal/llm"
)

// chatRequest is the OpenAI request shape we accept. Fields we don't model are
// ignored, except the ones in toLLM's rejections, which change the meaning of
// the request too much to drop silently.
type chatRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Temperature         *float64        `json:"temperature"`
	TopP                *float64        `json:"top_p"`
	MaxTokens           *int            `json:"max_tokens"`
	MaxCompletionTokens *int            `json:"max_completion_tokens"` // newer OpenAI name
	Stop                json.RawMessage `json:"stop"`                  // string or []string
	Stream              bool            `json:"stream"`
	StreamOptions       *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`

	Tools     json.RawMessage `json:"tools"`
	Functions json.RawMessage `json:"functions"`
	N         *int            `json:"n"`
}

type badRequest string

func (e badRequest) Error() string { return string(e) }

func (c *chatRequest) toLLM() (*llm.ChatRequest, error) {
	if len(c.Tools) > 0 || len(c.Functions) > 0 {
		return nil, badRequest("tools/function calling is not supported by this gateway yet")
	}
	if c.N != nil && *c.N != 1 {
		return nil, badRequest("n > 1 is not supported")
	}
	if c.Model == "" {
		return nil, badRequest("model is required")
	}
	if len(c.Messages) == 0 {
		return nil, badRequest("messages must not be empty")
	}
	r := &llm.ChatRequest{
		Model: c.Model, Temperature: c.Temperature, TopP: c.TopP,
		MaxTokens: c.MaxTokens, Stream: c.Stream,
	}
	if r.MaxTokens == nil {
		r.MaxTokens = c.MaxCompletionTokens
	}
	for i, m := range c.Messages {
		switch m.Role {
		case llm.RoleSystem, llm.RoleUser, llm.RoleAssistant:
		case "developer": // newer OpenAI alias for system
			m.Role = llm.RoleSystem
		default:
			return nil, badRequest(fmt.Sprintf("messages[%d]: unsupported role %q", i, m.Role))
		}
		text, err := textContent(m.Content)
		if err != nil {
			return nil, badRequest(fmt.Sprintf("messages[%d]: %v", i, err))
		}
		r.Messages = append(r.Messages, llm.Message{Role: m.Role, Content: text})
	}
	if len(c.Stop) > 0 {
		var one string
		if json.Unmarshal(c.Stop, &one) == nil {
			r.Stop = []string{one}
		} else if json.Unmarshal(c.Stop, &r.Stop) != nil {
			return nil, badRequest("stop must be a string or array of strings")
		}
	}
	return r, nil
}

// textContent accepts a plain string, or OpenAI's array-of-parts form as long
// as every part is text. Images etc. are rejected rather than dropped.
func textContent(raw json.RawMessage) (string, error) {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", errors.New("content must be a string or array of text parts")
	}
	var sb strings.Builder
	for _, p := range parts {
		if p.Type != "text" {
			return "", fmt.Errorf("content part type %q is not supported (text only)", p.Type)
		}
		sb.WriteString(p.Text)
	}
	return sb.String(), nil
}
