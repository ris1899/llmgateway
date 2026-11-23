package api

import (
	"encoding/json"
	"strings"
	"testing"
)

func decode(t *testing.T, body string) (*chatRequest, error) {
	t.Helper()
	var c chatRequest
	if err := json.Unmarshal([]byte(body), &c); err != nil {
		return nil, err
	}
	return &c, nil
}

func TestToLLM_Accepted(t *testing.T) {
	tests := []struct {
		name string
		body string
		want func(*testing.T, *chatRequest)
	}{
		{
			name: "minimal valid request",
			body: `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "all supported sampling params",
			body: `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],
				"temperature":0.5,"top_p":0.9,"max_tokens":128,
				"stop":["a","b"],"stream":true,
				"stream_options":{"include_usage":true}}`,
		},
		{
			name: "harmless metadata field user is accepted and ignored",
			body: `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"user":"abc123"}`,
		},
		{
			name: "developer role aliases to system",
			body: `{"model":"gpt-4","messages":[{"role":"developer","content":"sys"}]}`,
		},
		{
			name: "content as array of text parts",
			body: `{"model":"gpt-4","messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`,
		},
		{
			name: "stop null means no stop sequences",
			body: `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stop":null}`,
		},
		{
			name: "n equal to 1 is accepted",
			body: `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"n":1}`,
		},
		{
			name: "max_completion_tokens used when max_tokens absent",
			body: `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":64}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := decode(t, tt.body)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if _, err := c.toLLM(); err != nil {
				t.Fatalf("toLLM: unexpected error: %v", err)
			}
		})
	}
}

func TestToLLM_Rejected(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr string // substring expected in the error message
	}{
		{
			name:    "unknown top-level field",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"seed":42}`,
			wantErr: "unsupported parameter: seed",
		},
		{
			name:    "frequency_penalty rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"frequency_penalty":0.1}`,
			wantErr: "unsupported parameter: frequency_penalty",
		},
		{
			name:    "presence_penalty rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"presence_penalty":0.1}`,
			wantErr: "unsupported parameter: presence_penalty",
		},
		{
			name:    "logprobs rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"logprobs":true}`,
			wantErr: "unsupported parameter: logprobs",
		},
		{
			name:    "logit_bias rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"logit_bias":{"50256":-100}}`,
			wantErr: "unsupported parameter: logit_bias",
		},
		{
			name:    "response_format rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"response_format":{"type":"json_object"}}`,
			wantErr: "unsupported parameter: response_format",
		},
		{
			name:    "tools rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function"}]}`,
			wantErr: "tools/function calling",
		},
		{
			name:    "functions rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"functions":[{"name":"f"}]}`,
			wantErr: "tools/function calling",
		},
		{
			name:    "n greater than 1 rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"n":2}`,
			wantErr: "n > 1",
		},
		{
			name:    "image content part rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":[{"type":"image_url","text":""}]}]}`,
			wantErr: "not supported",
		},
		{
			name:    "missing model",
			body:    `{"messages":[{"role":"user","content":"hi"}]}`,
			wantErr: "model is required",
		},
		{
			name:    "empty messages",
			body:    `{"model":"gpt-4","messages":[]}`,
			wantErr: "messages must not be empty",
		},
		{
			name:    "invalid role",
			body:    `{"model":"gpt-4","messages":[{"role":"bogus","content":"hi"}]}`,
			wantErr: "unsupported role",
		},
		{
			name:    "max_tokens zero rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"max_tokens":0}`,
			wantErr: "max_tokens must be a positive integer",
		},
		{
			name:    "max_tokens negative rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"max_tokens":-5}`,
			wantErr: "max_tokens must be a positive integer",
		},
		{
			name:    "max_completion_tokens negative rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"max_completion_tokens":-1}`,
			wantErr: "max_completion_tokens must be a positive integer",
		},
		{
			name:    "temperature below range",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"temperature":-0.1}`,
			wantErr: "temperature must be between 0 and 2",
		},
		{
			name:    "temperature above range",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"temperature":2.1}`,
			wantErr: "temperature must be between 0 and 2",
		},
		{
			name:    "top_p zero rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"top_p":0}`,
			wantErr: "top_p must be between",
		},
		{
			name:    "top_p above one rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"top_p":1.1}`,
			wantErr: "top_p must be between",
		},
		{
			name:    "stop with empty string entry rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stop":["a",""]}`,
			wantErr: "empty strings",
		},
		{
			name:    "stop with more than four sequences rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stop":["a","b","c","d","e"]}`,
			wantErr: "at most 4",
		},
		{
			name:    "stop wrong type rejected",
			body:    `{"model":"gpt-4","messages":[{"role":"user","content":"hi"}],"stop":42}`,
			wantErr: "stop must be",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, decodeErr := decode(t, tt.body)
			var err error
			if decodeErr != nil {
				err = decodeErr
			} else {
				_, err = c.toLLM()
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got none", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestParseStop_StringAndArrayAndNull(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{name: "absent", raw: "", want: nil},
		{name: "null", raw: "null", want: nil},
		{name: "single string", raw: `"stop"`, want: []string{"stop"}},
		{name: "array of strings", raw: `["a","b"]`, want: []string{"a", "b"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseStop(json.RawMessage(tt.raw))
			if err != nil {
				t.Fatalf("parseStop: unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}
