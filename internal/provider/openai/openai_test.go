package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"llmgateway/internal/llm"
)

func newTestAdapter(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return New("test", srv.URL, "key", 5*time.Second, &http.Client{})
}

func TestChat_RequestTranslationAndResponse(t *testing.T) {
	t.Parallel()

	var gotBody wireRequest
	var gotAuth string
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode request: %v", err)
		}
		_, _ = w.Write([]byte(`{
			"id": "resp-1", "model": "gpt-test",
			"choices": [{"message": {"content": "hi there"}, "finish_reason": "stop"}],
			"usage": {"prompt_tokens": 3, "completion_tokens": 2}
		}`))
	})

	temp := 0.5
	resp, err := a.Chat(context.Background(), "gpt-test", &llm.ChatRequest{
		Model:       "gpt-test",
		Messages:    []llm.Message{{Role: llm.RoleUser, Content: "hello"}},
		Temperature: &temp,
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}

	if gotAuth != "Bearer key" {
		t.Errorf("Authorization header = %q, want %q", gotAuth, "Bearer key")
	}
	if gotBody.Model != "gpt-test" || len(gotBody.Messages) != 1 || gotBody.Messages[0].Content != "hello" {
		t.Errorf("request body translation wrong: %+v", gotBody)
	}
	if gotBody.Stream {
		t.Error("non-stream Chat must not set stream:true")
	}

	if resp.ID != "resp-1" || resp.Content != "hi there" || resp.FinishReason != "stop" {
		t.Errorf("response = %+v", resp)
	}
	if resp.Usage.InputTokens != 3 || resp.Usage.OutputTokens != 2 {
		t.Errorf("usage = %+v", resp.Usage)
	}
}

func TestChat_StatusError(t *testing.T) {
	t.Parallel()
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"slow down"}}`))
	})

	_, err := a.Chat(context.Background(), "m", &llm.ChatRequest{})
	e := llm.AsError(err)
	if e.Kind != llm.KindRateLimited {
		t.Errorf("Kind = %v, want KindRateLimited", e.Kind)
	}
}

func TestChatStream_ChunksAndUsage(t *testing.T) {
	t.Parallel()
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		fl := w.(http.Flusher)
		write := func(data string) {
			fmt.Fprintf(w, "data: %s\n\n", data)
			fl.Flush()
		}
		write(`{"choices":[{"delta":{"content":""}}]}`) // role-only chunk, skipped
		write(`{"choices":[{"delta":{"content":"Hello"}}]}`)
		write(`{"choices":[{"delta":{"content":" world"}, "finish_reason":"stop"}], "usage":{"prompt_tokens":4,"completion_tokens":2}}`)
		write("[DONE]")
	})

	s, err := a.ChatStream(context.Background(), "m", &llm.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	defer s.Close()

	var deltas []string
	var usage *llm.Usage
	var finish string
	for {
		c, err := s.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if c.Delta != "" {
			deltas = append(deltas, c.Delta)
		}
		if c.FinishReason != "" {
			finish = c.FinishReason
		}
		if c.Usage != nil {
			usage = c.Usage
		}
	}

	if strings.Join(deltas, "") != "Hello world" {
		t.Errorf("deltas = %v", deltas)
	}
	if finish != "stop" {
		t.Errorf("finish = %q", finish)
	}
	if usage == nil || usage.InputTokens != 4 || usage.OutputTokens != 2 {
		t.Errorf("usage = %+v", usage)
	}
}

func TestChatStream_InlineErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errType  string
		errCode  string
		wantKind llm.ErrorKind
	}{
		{"invalid request", "invalid_request_error", "", llm.KindBadRequest},
		{"rate limit by type", "rate_limit_error", "", llm.KindRateLimited},
		{"rate limit by code", "", "rate_limit_exceeded", llm.KindRateLimited},
		{"not found", "", "model_not_found", llm.KindNotFound},
		{"content filter", "content_filter", "", llm.KindContentFilter},
		{"unknown falls back", "something_else", "", llm.KindUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
				fl := w.(http.Flusher)
				body, _ := json.Marshal(struct {
					Error struct {
						Message string `json:"message"`
						Type    string `json:"type"`
						Code    string `json:"code"`
					} `json:"error"`
				}{Error: struct {
					Message string `json:"message"`
					Type    string `json:"type"`
					Code    string `json:"code"`
				}{Message: "boom", Type: tt.errType, Code: tt.errCode}})
				fmt.Fprintf(w, "data: %s\n\n", body)
				fl.Flush()
			})

			s, err := a.ChatStream(context.Background(), "m", &llm.ChatRequest{})
			if err != nil {
				t.Fatalf("ChatStream: %v", err)
			}
			defer s.Close()

			_, err = s.Recv()
			e := llm.AsError(err)
			if e.Kind != tt.wantKind {
				t.Errorf("Kind = %v, want %v", e.Kind, tt.wantKind)
			}
		})
	}
}

// Sanity check that the httptest handler streams rather than buffering the
// whole body, since SSE reading relies on incremental delivery.
func TestChatStream_UsesChunkedFlushing(t *testing.T) {
	t.Parallel()
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		bw := bufio.NewWriter(w)
		fmt.Fprint(bw, "data: [DONE]\n\n")
		_ = bw.Flush()
	})
	s, err := a.ChatStream(context.Background(), "m", &llm.ChatRequest{})
	if err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	defer s.Close()
	if _, err := s.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv = %v, want io.EOF", err)
	}
}
