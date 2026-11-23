package llm

import (
	"errors"
	"fmt"
	"testing"
)

func TestErrorClassification(t *testing.T) {
	tests := []struct {
		kind          ErrorKind
		retryable     bool
		countsFailure bool
	}{
		{KindTimeout, true, true},
		{KindUnavailable, true, true},
		{KindRateLimited, true, true},
		{KindAuth, true, true},
		{KindUnknown, true, true},
		{KindBadRequest, false, false},
		{KindContentFilter, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.kind.String(), func(t *testing.T) {
			e := &Error{Kind: tt.kind}
			if got := e.Retryable(); got != tt.retryable {
				t.Errorf("Retryable() = %v, want %v", got, tt.retryable)
			}
			if got := e.CountsAsFailure(); got != tt.countsFailure {
				t.Errorf("CountsAsFailure() = %v, want %v", got, tt.countsFailure)
			}
		})
	}
}

func TestAsError(t *testing.T) {
	orig := &Error{Provider: "p", Kind: KindTimeout}
	wrapped := fmt.Errorf("attempt 1: %w", orig)
	if got := AsError(wrapped); got != orig {
		t.Fatalf("AsError did not unwrap to original: %+v", got)
	}
	if got := AsError(errors.New("boom")); got.Kind != KindUnknown {
		t.Fatalf("plain error kind = %v, want unknown", got.Kind)
	}
}
