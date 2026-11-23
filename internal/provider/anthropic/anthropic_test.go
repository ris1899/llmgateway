package anthropic

import (
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

func TestToWire_SystemPromptExtractionAndJoin(t *testing.T) {
	t.Parallel()
	req := &llm.ChatRequest{
		Messages: []llm.Message{
			{Role: llm.RoleSystem, Content: "be nice"},
			{Role: llm.RoleUser, Content: "hi"},
			{Role: llm.RoleSystem, Content: "be brief"},
			{Role: llm.RoleAssistant, Content: "hello"},
		},
	}
	w := toWire("claude-test", req, false)

	if w.System != "be nice\n\nbe brief" {
		t.Errorf("System = %q", w.System)
	}
	if len(w.Messages) != 2 || w.Messages[0].Role != llm.RoleUser || w.Messages[1].Role != llm.RoleAssistant {
		t.Errorf("Messages = %+v, system prompts should be excluded", w.Messages)
	}
}

func TestToWire_MaxTokensDefaultAndOverride(t *testing.T) {
	t.Parallel()

	w := toWire("m", &llm.ChatRequest{}, false)
	if w.MaxTokens != defaultMaxTokens {
		t.Errorf("MaxTokens = %d, want default %d", w.MaxTokens, defaultMaxTokens)
	}

	explicit := 42
	w = toWire("m", &llm.ChatRequest{MaxTokens: &explicit}, false)
	if w.MaxTokens != 42 {
		t.Errorf("MaxTokens = %d, want client-supplied 42", w.MaxTokens)
	}
}

func TestFinishReason_AllKnownValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want string
	}{
		{"end_turn", "stop"},
		{"stop_sequence", "stop"},
		{"max_tokens", "length"},
		{"refusal", "content_filter"},
		{"tool_use", "tool_calls"},
		{"pause_turn", "stop"},
		{"some_future_reason", "stop"}, // must never leak a raw Anthropic string
	}
	for _, tt := range tests {
		if got := finishReason(tt.in); got != tt.want {
			t.Errorf("finishReason(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestChat_RequestAndResponseTranslation(t *testing.T) {
	t.Parallel()
	var gotVersion, gotKey string
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		gotVersion = r.Header.Get("anthropic-version")
		gotKey = r.Header.Get("x-api-key")
		_, _ = w.Write([]byte(`{
			"id": "msg-1", "model": "claude-test",
			"content": [{"type":"text","text":"hel"}, {"type":"text","text":"lo"}],
			"stop_reason": "end_turn",
			"usage": {"input_tokens": 5, "output_tokens": 7}
		}`))
	})

	resp, err := a.Chat(context.Background(), "claude-test", &llm.ChatRequest{
		Messages: []llm.Message{{Role: llm.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	if gotVersion != apiVersion {
		t.Errorf("anthropic-version = %q, want %q", gotVersion, apiVersion)
	}
	if gotKey != "key" {
		t.Errorf("x-api-key = %q, want %q", gotKey, "key")
	}
	if resp.Content != "hello" || resp.FinishReason != "stop" {
		t.Errorf("response = %+v", resp)
	}
	if resp.Usage.InputTokens != 5 || resp.Usage.OutputTokens != 7 {
		t.Errorf("usage = %+v", resp.Usage)
	}
}

func TestChatStream_EventsAndUsage(t *testing.T) {
	t.Parallel()
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		fl := w.(http.Flusher)
		write := func(data string) {
			fmt.Fprintf(w, "data: %s\n\n", data)
			fl.Flush()
		}
		write(`{"type":"message_start","message":{"usage":{"input_tokens":10}}}`)
		write(`{"type":"ping"}`)
		write(`{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}`)
		write(`{"type":"content_block_delta","delta":{"type":"text_delta","text":" there"}}`)
		write(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`)
		write(`{"type":"message_stop"}`)
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

	if strings.Join(deltas, "") != "Hi there" {
		t.Errorf("deltas = %v", deltas)
	}
	if finish != "stop" {
		t.Errorf("finish = %q", finish)
	}
	if usage == nil || usage.InputTokens != 10 || usage.OutputTokens != 3 {
		t.Errorf("usage = %+v, want input 10 (from message_start) + output 3 (from message_delta)", usage)
	}
}

func TestChatStream_ErrorEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errType  string
		wantKind llm.ErrorKind
	}{
		{"rate limit", "rate_limit_error", llm.KindRateLimited},
		{"overloaded falls back", "overloaded_error", llm.KindUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
				fl := w.(http.Flusher)
				body, _ := json.Marshal(map[string]any{
					"type": "error",
					"error": map[string]string{
						"type":    tt.errType,
						"message": "boom",
					},
				})
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
