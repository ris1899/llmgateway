package usage

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"
)

func TestEventRoundTripsThroughJSON(t *testing.T) {
	t.Parallel()
	e := Event{
		EventID: NewEventID(), TS: time.Now().UTC().Truncate(time.Millisecond),
		RequestID: "req_1", KeyID: "key_1", Model: "gpt-group", Deployment: "dep-a",
		Status: "ok", Stream: true, CacheHit: false, Attempts: 2,
		InputTokens: 10, OutputTokens: 20, Estimated: true,
		LatencyMs: 123.4, TTFTMs: 12.3, CostUSD: 0.0042,
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got Event
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if got != e {
		t.Errorf("round trip mismatch:\n got  %+v\n want %+v", got, e)
	}
}

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewEventIDIsUUIDv4(t *testing.T) {
	t.Parallel()
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewEventID()
		if !uuidV4.MatchString(id) {
			t.Fatalf("NewEventID() = %q, not a valid UUIDv4", id)
		}
		if seen[id] {
			t.Fatalf("NewEventID() produced a duplicate: %q", id)
		}
		seen[id] = true
	}
}

func TestNoopSinkDoesNothing(t *testing.T) {
	t.Parallel()
	var s Sink = Noop{}
	s.Emit(Event{}) // must not panic
	s.Close()       // must not panic
}
