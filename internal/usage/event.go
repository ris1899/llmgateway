// Package usage emits per-request events to Kafka and aggregates them into Postgres.
package usage

import (
	"crypto/rand"
	"fmt"
	"time"
)

type Event struct {
	EventID      string    `json:"event_id"` // UUIDv4, the dedupe key downstream
	TS           time.Time `json:"ts"`
	RequestID    string    `json:"request_id"`
	KeyID        string    `json:"key_id"`
	Model        string    `json:"model"`
	Deployment   string    `json:"deployment"`
	Status       string    `json:"status"` // ok, or an error kind
	Stream       bool      `json:"stream"`
	CacheHit     bool      `json:"cache_hit"`
	Attempts     int       `json:"attempts"`
	InputTokens  int       `json:"input_tokens"`
	OutputTokens int       `json:"output_tokens"`
	Estimated    bool      `json:"estimated"`
	LatencyMs    float64   `json:"latency_ms"`
	TTFTMs       float64   `json:"ttft_ms"`
	CostUSD      float64   `json:"cost_usd"`
}

func NewEventID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type Sink interface {
	Emit(e Event)
	Close()
}

type Noop struct{}

func (Noop) Emit(Event) {}
func (Noop) Close()     {}
