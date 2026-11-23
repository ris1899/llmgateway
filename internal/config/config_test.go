package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) { v, ok := m[k]; return v, ok }
}

const valid = `
providers:
  mock:
    type: openai
    base_url: http://localhost:9000/v1
  openai:
    type: openai
    base_url: https://api.openai.com/v1
    api_key_env: OPENAI_API_KEY
    timeout: 30s
models:
  fast-chat:
    deployments:
      - provider: mock
        model: mock-small
        input_price_per_m: 0.1
        output_price_per_m: 0.2
      - provider: openai
        model: gpt-4o-mini
        priority: 1
        weight: 3
`

func TestParseValid(t *testing.T) {
	c, err := Parse([]byte(valid), env(map[string]string{"OPENAI_API_KEY": "sk-test"}))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Server.Addr != ":8080" || c.Server.MaxBodyBytes != 4<<20 {
		t.Errorf("server defaults not applied: %+v", c.Server)
	}
	if got := c.Providers["openai"].APIKey; got != "sk-test" {
		t.Errorf("api key not resolved, got %q", got)
	}
	if got := c.Providers["openai"].Timeout; got != 30*time.Second {
		t.Errorf("explicit timeout overwritten: %v", got)
	}
	if got := c.Providers["mock"].Timeout; got != 60*time.Second {
		t.Errorf("default timeout = %v", got)
	}
	deps := c.Models["fast-chat"].Deployments
	if deps[0].Weight != 1 || deps[1].Weight != 3 {
		t.Errorf("weights = %d,%d want 1,3", deps[0].Weight, deps[1].Weight)
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		env     map[string]string
		wantErr []string // every substring must appear: we report all errors at once
	}{
		{
			name:    "missing secret",
			yaml:    valid,
			wantErr: []string{"OPENAI_API_KEY is not set"},
		},
		{
			name:    "unknown field is rejected (typo protection)",
			yaml:    "server:\n  adr: :8080\n",
			wantErr: []string{"field adr not found"},
		},
		{
			name: "multiple problems reported together",
			yaml: `
providers:
  bad:
    type: cohere
models:
  m:
    deployments:
      - provider: nope
        input_price_per_m: -1
  empty: {}
`,
			wantErr: []string{
				`unknown type "cohere"`,
				`base_url is required`,
				`unknown provider "nope"`,
				`model is required`,
				`prices must be >= 0`,
				`model "empty": no deployments`,
			},
		},
		{
			name:    "empty config",
			yaml:    "{}",
			wantErr: []string{"no providers configured", "no models configured"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml), env(tt.env))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q missing %q", err, want)
				}
			}
		})
	}
}

// The example config shipped in the repo must always load, so a new developer's
// first run never fails on a stale example.
func TestExampleConfigLoads(t *testing.T) {
	path := filepath.Join("..", "..", "config.example.yaml")
	if _, err := os.Stat(path); err != nil {
		t.Skip("config.example.yaml not found")
	}
	if _, err := Load(path, env(nil)); err != nil {
		t.Fatalf("config.example.yaml invalid: %v", err)
	}
}
