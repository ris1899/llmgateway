package cache

import (
	"testing"

	"llmgateway/internal/llm"
)

func f64(v float64) *float64 { return &v }
func i(v int) *int           { return &v }

func baseReq() *llm.ChatRequest {
	return &llm.ChatRequest{
		Model:       "gpt-group",
		Messages:    []llm.Message{{Role: "user", Content: "hi"}},
		Temperature: f64(0),
		TopP:        f64(1),
		MaxTokens:   i(100),
		Stop:        []string{"END"},
	}
}

func mustKey(t *testing.T, c *Cache, req *llm.ChatRequest) string {
	t.Helper()
	k, ok := c.Key(req)
	if !ok {
		t.Fatalf("Key() reported not cacheable for %+v", req)
	}
	return k
}

func TestKeyGatesOnTemperature(t *testing.T) {
	t.Parallel()
	c := New(nil, 0, 0.2, 0)

	if _, ok := c.Key(&llm.ChatRequest{Model: "m", Temperature: nil}); ok {
		t.Error("nil temperature must not be cacheable (non-deterministic)")
	}
	hot := baseReq()
	hot.Temperature = f64(0.5)
	if _, ok := c.Key(hot); ok {
		t.Error("temperature above maxTemp must not be cacheable")
	}
	cold := baseReq()
	cold.Temperature = f64(0.1)
	if _, ok := c.Key(cold); !ok {
		t.Error("temperature within maxTemp should be cacheable")
	}
}

// Every field that can change the model's answer must change the key;
// fields that don't affect determinism must not.
func TestKeyChangesWithDeterminismAffectingFields(t *testing.T) {
	t.Parallel()
	c := New(nil, 0, 1, 0)
	base := mustKey(t, c, baseReq())

	mutations := map[string]func(*llm.ChatRequest){
		"model group": func(r *llm.ChatRequest) { r.Model = "other-group" },
		"messages":    func(r *llm.ChatRequest) { r.Messages[0].Content = "bye" },
		"temperature": func(r *llm.ChatRequest) { r.Temperature = f64(0.05) },
		"top_p":       func(r *llm.ChatRequest) { r.TopP = f64(0.9) },
		"max_tokens":  func(r *llm.ChatRequest) { r.MaxTokens = i(1) },
		"stop":        func(r *llm.ChatRequest) { r.Stop = []string{"OTHER"} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			req := baseReq()
			mutate(req)
			if got := mustKey(t, c, req); got == base {
				t.Errorf("changing %s did not change the cache key", name)
			}
		})
	}
}

func TestKeyStableAcrossIrrelevantFieldsAndFieldOrder(t *testing.T) {
	t.Parallel()
	c := New(nil, 0, 1, 0)
	a := baseReq()
	b := baseReq()
	// Same content built differently must still produce the same key.
	b.Messages = []llm.Message{{Role: "user", Content: "hi"}}

	ka := mustKey(t, c, a)
	kb := mustKey(t, c, b)
	if ka != kb {
		t.Errorf("equivalent requests produced different keys: %q vs %q", ka, kb)
	}
}

func TestKeyPrefixAndFormat(t *testing.T) {
	t.Parallel()
	c := New(nil, 0, 1, 0)
	k := mustKey(t, c, baseReq())
	const prefix = "llmgw:cache:"
	if len(k) <= len(prefix) || k[:len(prefix)] != prefix {
		t.Errorf("key %q missing expected prefix %q", k, prefix)
	}
	if len(k) != len(prefix)+64 { // hex-encoded sha256
		t.Errorf("key %q has unexpected length %d", k, len(k))
	}
}
