// Package anthropic adapts the Anthropic Messages API. The differences from
// OpenAI that matter:
//   - system prompt is a top-level field, not a message
//   - max_tokens is required
//   - streaming uses named events; text comes in content_block_delta, input
//     tokens in message_start and output tokens in message_delta
//   - errors can also arrive mid-stream as an "error" event (e.g. overloaded)
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"llmgateway/internal/llm"
	"llmgateway/internal/provider"
)

const apiVersion = "2023-06-01"

// Anthropic requires max_tokens; OpenAI-style clients often omit it. This
// default keeps those requests working while bounding cost.
const defaultMaxTokens = 1024

type Adapter struct {
	name    string
	baseURL string
	apiKey  string
	timeout time.Duration
	hc      *http.Client
}

func New(name, baseURL, apiKey string, timeout time.Duration, hc *http.Client) *Adapter {
	return &Adapter{name: name, baseURL: baseURL, apiKey: apiKey, timeout: timeout, hc: hc}
}

func (a *Adapter) Name() string { return a.name }

type wireMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type wireRequest struct {
	Model         string        `json:"model"`
	System        string        `json:"system,omitempty"`
	Messages      []wireMessage `json:"messages"`
	MaxTokens     int           `json:"max_tokens"`
	Temperature   *float64      `json:"temperature,omitempty"`
	TopP          *float64      `json:"top_p,omitempty"`
	StopSequences []string      `json:"stop_sequences,omitempty"`
	Stream        bool          `json:"stream,omitempty"`
}

type wireUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

// Map Anthropic stop reasons onto OpenAI's vocabulary, since our public API
// speaks OpenAI.
func finishReason(s string) string {
	switch s {
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "refusal":
		return "content_filter"
	default:
		return s
	}
}

func toWire(model string, req *llm.ChatRequest, stream bool) wireRequest {
	w := wireRequest{
		Model: model, MaxTokens: defaultMaxTokens, Temperature: req.Temperature,
		TopP: req.TopP, StopSequences: req.Stop, Stream: stream,
	}
	if req.MaxTokens != nil {
		w.MaxTokens = *req.MaxTokens
	}
	var system []string
	for _, m := range req.Messages {
		if m.Role == llm.RoleSystem {
			system = append(system, m.Content)
			continue
		}
		w.Messages = append(w.Messages, wireMessage{Role: m.Role, Content: m.Content})
	}
	w.System = strings.Join(system, "\n\n")
	return w
}

func (a *Adapter) build(model string, req *llm.ChatRequest, stream bool) func(context.Context) (*http.Request, error) {
	return func(ctx context.Context) (*http.Request, error) {
		body, err := json.Marshal(toWire(model, req, stream))
		if err != nil {
			return nil, err
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/messages", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("anthropic-version", apiVersion)
		if a.apiKey != "" {
			r.Header.Set("x-api-key", a.apiKey)
		}
		return r, nil
	}
}

func (a *Adapter) Chat(ctx context.Context, model string, req *llm.ChatRequest) (*llm.ChatResponse, error) {
	resp, dl, err := provider.Do(a.hc, a.name, a.timeout, ctx, a.build(model, req, false))
	if err != nil {
		return nil, err
	}
	defer dl.Release()
	defer resp.Body.Close()

	var w struct {
		ID      string `json:"id"`
		Model   string `json:"model"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		StopReason string    `json:"stop_reason"`
		Usage      wireUsage `json:"usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&w); err != nil {
		return nil, provider.TransportError(a.name, err, dl)
	}
	var sb strings.Builder
	for _, c := range w.Content {
		if c.Type == "text" {
			sb.WriteString(c.Text)
		}
	}
	return &llm.ChatResponse{
		ID: w.ID, Model: w.Model, Content: sb.String(),
		FinishReason: finishReason(w.StopReason),
		Usage:        llm.Usage{InputTokens: w.Usage.InputTokens, OutputTokens: w.Usage.OutputTokens},
	}, nil
}

func (a *Adapter) ChatStream(ctx context.Context, model string, req *llm.ChatRequest) (provider.Stream, error) {
	resp, dl, err := provider.Do(a.hc, a.name, a.timeout, ctx, a.build(model, req, true))
	if err != nil {
		return nil, err
	}
	return &stream{a: a, body: resp.Body, sse: provider.NewSSEReader(resp.Body), dl: dl}, nil
}

type stream struct {
	a           *Adapter
	body        io.ReadCloser
	sse         *provider.SSEReader
	dl          *provider.Deadline
	inputTokens int // from message_start, combined with output at message_delta
}

type event struct {
	Type    string `json:"type"`
	Message struct {
		Usage wireUsage `json:"usage"`
	} `json:"message"`
	Delta struct {
		Type       string `json:"type"`
		Text       string `json:"text"`
		StopReason string `json:"stop_reason"`
	} `json:"delta"`
	Usage *wireUsage `json:"usage"`
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
}

func (s *stream) Recv() (llm.StreamChunk, error) {
	for {
		ev, err := s.sse.Next()
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF // must end with message_stop
			}
			return llm.StreamChunk{}, provider.TransportError(s.a.name, err, s.dl)
		}
		s.dl.Extend()
		var e event
		if err := json.Unmarshal(ev.Data, &e); err != nil {
			return llm.StreamChunk{}, &llm.Error{Provider: s.a.name, Kind: llm.KindUnknown, Err: err}
		}
		switch e.Type {
		case "message_start":
			s.inputTokens = e.Message.Usage.InputTokens
		case "content_block_delta":
			if e.Delta.Type == "text_delta" && e.Delta.Text != "" {
				return llm.StreamChunk{Delta: e.Delta.Text}, nil
			}
		case "message_delta":
			c := llm.StreamChunk{FinishReason: finishReason(e.Delta.StopReason)}
			if e.Usage != nil {
				c.Usage = &llm.Usage{InputTokens: s.inputTokens, OutputTokens: e.Usage.OutputTokens}
			}
			return c, nil
		case "message_stop":
			return llm.StreamChunk{}, io.EOF
		case "error":
			kind := llm.KindUnavailable
			if e.Error != nil && e.Error.Type == "rate_limit_error" {
				kind = llm.KindRateLimited
			}
			msg := "stream error"
			if e.Error != nil {
				msg = e.Error.Message
			}
			return llm.StreamChunk{}, &llm.Error{Provider: s.a.name, Kind: kind, Err: errors.New(msg)}
		}
		// ping, content_block_start/stop: nothing to forward
	}
}

func (s *stream) Close() error {
	s.dl.Release()
	return s.body.Close()
}
