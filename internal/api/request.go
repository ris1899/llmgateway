package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"llmgateway/internal/llm"
)

// chatRequest is the OpenAI request shape we accept. UnmarshalJSON allowlists
// every top-level field, rejecting unknowns with 400 instead of silently
// dropping them (e.g. a client expecting seed/logit_bias to take effect).
type chatRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	Temperature         *float64        `json:"temperature"`
	TopP                *float64        `json:"top_p"`
	MaxTokens           *int            `json:"max_tokens"`
	MaxCompletionTokens *int            `json:"max_completion_tokens"`
	Stop                json.RawMessage `json:"stop"` // string, []string, or null
	Stream              bool            `json:"stream"`
	StreamOptions       *struct {
		IncludeUsage bool `json:"include_usage"`
	} `json:"stream_options"`

	Tools     json.RawMessage `json:"tools"`
	Functions json.RawMessage `json:"functions"`
	N         *int            `json:"n"`
}

// keep in sync with chatRequest's json tags
var chatRequestFields = map[string]bool{
	"model":                 true,
	"messages":              true,
	"temperature":           true,
	"top_p":                 true,
	"max_tokens":            true,
	"max_completion_tokens": true,
	"stop":                  true,
	"stream":                true,
	"stream_options":        true,
	"tools":                 true,
	"functions":             true,
	"n":                     true,
	"user":                  true, // harmless metadata; accepted and ignored
}

func (c *chatRequest) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	for field := range raw {
		if !chatRequestFields[field] {
			return badRequest(fmt.Sprintf("unsupported parameter: %s", field))
		}
	}
	type alias chatRequest // avoids recursing into UnmarshalJSON
	return json.Unmarshal(data, (*alias)(c))
}

type badRequest string

func (e badRequest) Error() string { return string(e) }

const maxStopSequences = 4 // OpenAI's own limit

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
	if c.Temperature != nil && (*c.Temperature < 0 || *c.Temperature > 2) {
		return nil, badRequest("temperature must be between 0 and 2")
	}
	if c.TopP != nil && (*c.TopP <= 0 || *c.TopP > 1) {
		return nil, badRequest("top_p must be between 0 (exclusive) and 1")
	}
	if c.MaxTokens != nil && *c.MaxTokens <= 0 {
		return nil, badRequest("max_tokens must be a positive integer")
	}
	if c.MaxCompletionTokens != nil && *c.MaxCompletionTokens <= 0 {
		return nil, badRequest("max_completion_tokens must be a positive integer")
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

	stop, err := parseStop(c.Stop)
	if err != nil {
		return nil, err
	}
	r.Stop = stop

	return r, nil
}

// null must be special-cased first: it unmarshals into a Go string as "",
// which would otherwise become a single empty stop sequence.
func parseStop(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var one string
	var stop []string
	if json.Unmarshal(raw, &one) == nil {
		stop = []string{one}
	} else if json.Unmarshal(raw, &stop) != nil {
		return nil, badRequest("stop must be a string, array of strings, or null")
	}
	if len(stop) > maxStopSequences {
		return nil, badRequest(fmt.Sprintf("stop supports at most %d sequences", maxStopSequences))
	}
	for _, s := range stop {
		if s == "" {
			return nil, badRequest("stop sequences must not be empty strings")
		}
	}
	return stop, nil
}

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
