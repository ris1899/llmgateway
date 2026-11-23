// Package openai adapts any OpenAI-compatible Chat Completions API: OpenAI
// itself, OpenRouter, Groq, vLLM, Ollama and our mock. Only base_url and the
// key differ, so one adapter covers all of them.
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"llmgateway/internal/llm"
	"llmgateway/internal/provider"
)

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

// --- wire types ---

type wireRequest struct {
	Model         string        `json:"model"`
	Messages      []llm.Message `json:"messages"` // our Message is already OpenAI-shaped
	Temperature   *float64      `json:"temperature,omitempty"`
	TopP          *float64      `json:"top_p,omitempty"`
	MaxTokens     *int          `json:"max_tokens,omitempty"`
	Stop          []string      `json:"stop,omitempty"`
	Stream        bool          `json:"stream,omitempty"`
	StreamOptions *streamOpts   `json:"stream_options,omitempty"`
}

type streamOpts struct {
	IncludeUsage bool `json:"include_usage"`
}

type wireUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

type wireResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *wireUsage `json:"usage"`
	// Some compatible servers send errors inside the stream as data events.
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (a *Adapter) build(model string, req *llm.ChatRequest, stream bool) func(context.Context) (*http.Request, error) {
	return func(ctx context.Context) (*http.Request, error) {
		w := wireRequest{
			Model: model, Messages: req.Messages, Temperature: req.Temperature,
			TopP: req.TopP, MaxTokens: req.MaxTokens, Stop: req.Stop, Stream: stream,
		}
		if stream {
			// Without this, OpenAI sends no token counts in streams and we could
			// not bill streamed requests accurately.
			w.StreamOptions = &streamOpts{IncludeUsage: true}
		}
		body, err := json.Marshal(w)
		if err != nil {
			return nil, err
		}
		r, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+"/chat/completions", bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		r.Header.Set("Content-Type", "application/json")
		if a.apiKey != "" {
			r.Header.Set("Authorization", "Bearer "+a.apiKey)
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

	var w wireResponse
	if err := json.NewDecoder(resp.Body).Decode(&w); err != nil {
		return nil, provider.TransportError(a.name, err, dl)
	}
	if len(w.Choices) == 0 {
		return nil, &llm.Error{Provider: a.name, Kind: llm.KindUnknown, Err: errors.New("no choices in response")}
	}
	out := &llm.ChatResponse{ID: w.ID, Model: w.Model, Content: w.Choices[0].Message.Content}
	if f := w.Choices[0].FinishReason; f != nil {
		out.FinishReason = *f
	}
	if w.Usage != nil {
		out.Usage = llm.Usage{InputTokens: w.Usage.PromptTokens, OutputTokens: w.Usage.CompletionTokens}
	}
	return out, nil
}

func (a *Adapter) ChatStream(ctx context.Context, model string, req *llm.ChatRequest) (provider.Stream, error) {
	resp, dl, err := provider.Do(a.hc, a.name, a.timeout, ctx, a.build(model, req, true))
	if err != nil {
		return nil, err
	}
	return &stream{a: a, body: resp.Body, sse: provider.NewSSEReader(resp.Body), dl: dl}, nil
}

type stream struct {
	a    *Adapter
	body io.ReadCloser
	sse  *provider.SSEReader
	dl   *provider.Deadline
}

func (s *stream) Recv() (llm.StreamChunk, error) {
	for {
		ev, err := s.sse.Next()
		if err != nil {
			// A body that ends without [DONE] is a cut stream, not a clean finish.
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return llm.StreamChunk{}, provider.TransportError(s.a.name, err, s.dl)
		}
		s.dl.Extend()
		if string(ev.Data) == "[DONE]" {
			return llm.StreamChunk{}, io.EOF
		}
		var w wireResponse
		if err := json.Unmarshal(ev.Data, &w); err != nil {
			return llm.StreamChunk{}, &llm.Error{Provider: s.a.name, Kind: llm.KindUnknown, Err: fmt.Errorf("bad chunk: %w", err)}
		}
		if w.Error != nil {
			return llm.StreamChunk{}, &llm.Error{Provider: s.a.name, Kind: llm.KindUnavailable, Err: errors.New(w.Error.Message)}
		}
		var c llm.StreamChunk
		if len(w.Choices) > 0 {
			c.Delta = w.Choices[0].Delta.Content
			if f := w.Choices[0].FinishReason; f != nil {
				c.FinishReason = *f
			}
		}
		if w.Usage != nil {
			c.Usage = &llm.Usage{InputTokens: w.Usage.PromptTokens, OutputTokens: w.Usage.CompletionTokens}
		}
		// Skip empty chunks (e.g. the role-only first delta) so that the first
		// chunk the caller sees is real content.
		if c.Delta == "" && c.FinishReason == "" && c.Usage == nil {
			continue
		}
		return c, nil
	}
}

func (s *stream) Close() error {
	s.dl.Release()
	return s.body.Close()
}
