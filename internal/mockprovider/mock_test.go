package mockprovider

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// patchBehavior issues the PATCH and returns the status and decoded body
// (body only valid when status is 200).
func patchBehavior(t *testing.T, h http.Handler, body string) (int, Behavior) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPatch, "/admin/behavior", strings.NewReader(body))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	var b Behavior
	if rr.Code == http.StatusOK {
		if err := json.Unmarshal(rr.Body.Bytes(), &b); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
	return rr.Code, b
}

func getBehavior(t *testing.T, h http.Handler) Behavior {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/behavior", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	var b Behavior
	if err := json.Unmarshal(rr.Body.Bytes(), &b); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return b
}

// exercises the deterministic edges (0 or 1) so it never depends on the RNG
func TestPatchBehaviorErrorRate(t *testing.T) {
	srv, err := New("mock", DefaultBehavior())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := srv.Handler()

	status, b := patchBehavior(t, h, `{"error_rate":1,"error_status":503}`)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200", status)
	}
	if b.ErrorRate != 1 || b.ErrorStatus != 503 {
		t.Fatalf("behavior = %+v, want error_rate=1 error_status=503", b)
	}

	chatReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(`{"model":"mock-small","messages":[{"role":"user","content":"hi"}]}`)))
	chatReq.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, chatReq)
	if rr.Code != 503 {
		t.Fatalf("chat completion status = %d, want 503", rr.Code)
	}

	status2, b2 := patchBehavior(t, h, `{"error_rate":0}`)
	if status2 != http.StatusOK || b2.ErrorRate != 0 {
		t.Fatalf("reset error_rate failed: status=%d body=%+v", status2, b2)
	}
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(`{"model":"mock-small","messages":[{"role":"user","content":"hi"}]}`))))
	if rr2.Code != http.StatusOK {
		t.Fatalf("chat completion status = %d, want 200 after resetting error_rate", rr2.Code)
	}
}

// unrelated fields must survive a partial PATCH untouched
func TestPatchBehaviorMerge(t *testing.T) {
	srv, err := New("mock", DefaultBehavior())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := srv.Handler()

	before := getBehavior(t, h)
	_, after := patchBehavior(t, h, `{"error_status":429}`)
	if after.ErrorStatus != 429 {
		t.Fatalf("error_status not applied: %+v", after)
	}
	if after.OutputTokens != before.OutputTokens || after.TokensPerSec != before.TokensPerSec {
		t.Fatalf("unrelated fields changed: before=%+v after=%+v", before, after)
	}
}

// toggles a fault hang -> reset -> none, and checks an invalid value is rejected
func TestPatchBehaviorFaultHangThenReset(t *testing.T) {
	srv, err := New("mock", DefaultBehavior())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := srv.Handler()

	_, b := patchBehavior(t, h, `{"fault":"hang"}`)
	if b.Fault != "hang" {
		t.Fatalf("fault = %q, want hang", b.Fault)
	}

	_, b = patchBehavior(t, h, `{"fault":"reset"}`)
	if b.Fault != "reset" {
		t.Fatalf("fault = %q, want reset", b.Fault)
	}

	status, _ := patchBehavior(t, h, `{"fault":"bogus"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("invalid fault status = %d, want 400", status)
	}
	if cur := getBehavior(t, h); cur.Fault != "reset" {
		t.Fatalf("fault changed by rejected patch: %q", cur.Fault)
	}

	_, b = patchBehavior(t, h, `{"fault":""}`)
	if b.Fault != "" {
		t.Fatalf("fault = %q, want cleared", b.Fault)
	}
}

// the streaming abort itself is exercised in bench/failover.sh, not here
func TestPatchBehaviorFailAfterTokens(t *testing.T) {
	srv, err := New("mock", DefaultBehavior())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h := srv.Handler()

	_, b := patchBehavior(t, h, `{"fail_after_tokens":3}`)
	if b.FailAfterTokens != 3 {
		t.Fatalf("fail_after_tokens = %d, want 3", b.FailAfterTokens)
	}
}

func TestPatchBehaviorInvalidJSON(t *testing.T) {
	srv, err := New("mock", DefaultBehavior())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	status, _ := patchBehavior(t, srv.Handler(), `not json`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
}
