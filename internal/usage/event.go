// Package usage emits one event per request to Kafka and (in the consumer)
// aggregates them into Postgres.
package usage

import (
	"crypto/rand"
	"fmt"
	"time"
)

// Event is the wire format (JSON) on the usage topic.
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

// Sink receives events. The gateway uses a Kafka producer or a no-op.
type Sink interface {
	Emit(e Event)
	Close()
}

type Noop struct{}

func (Noop) Emit(Event) {}
func (Noop) Close()     {}
