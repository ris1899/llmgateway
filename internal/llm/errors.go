package llm

import (
	"errors"
	"fmt"
	"time"
)

// ErrorKind classifies upstream failures. Adapters map provider-specific
// status codes and error bodies into these kinds; the router only ever looks at
// the kind. This is what lets failover logic stay provider-agnostic.
type ErrorKind int

const (
	KindUnknown       ErrorKind = iota
	KindTimeout                 // deadline exceeded talking to upstream
	KindUnavailable             // 5xx, connection refused/reset
	KindRateLimited             // 429 from the provider (not our own limiter)
	KindBadRequest              // 400/422: the request itself is invalid
	KindAuth                    // 401/403: our provider credentials are wrong
	KindContentFilter           // provider refused on safety grounds
	KindCanceled                // our side gave up (client disconnected); says nothing about the provider
)

func (k ErrorKind) String() string {
	switch k {
	case KindTimeout:
		return "timeout"
	case KindUnavailable:
		return "unavailable"
	case KindRateLimited:
		return "rate_limited"
	case KindBadRequest:
		return "bad_request"
	case KindAuth:
		return "auth"
	case KindContentFilter:
		return "content_filter"
	case KindCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// Error is the single error type adapters return for upstream failures.
type Error struct {
	Provider   string
	Kind       ErrorKind
	Status     int           // upstream HTTP status, 0 if none (e.g. dial error)
	RetryAfter time.Duration // from Retry-After header, 0 if absent
	Err        error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (status %d): %v", e.Provider, e.Kind, e.Status, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Retryable reports whether trying another provider could plausibly succeed.
//
// BadRequest and ContentFilter are about the request, so every provider would
// reject it too; retrying just burns money and latency. Auth is retryable on a
// *different* provider (our key for this one is broken, others may be fine).
// Unknown is treated as retryable: failing over on a surprise error is the safer
// default for a gateway whose job is availability.
func (e *Error) Retryable() bool {
	switch e.Kind {
	case KindBadRequest, KindContentFilter, KindCanceled:
		return false
	default:
		return true
	}
}

// CountsAsFailure reports whether this error should hurt the provider's health
// (circuit breaker, error rate). A client's malformed request says nothing about
// the provider, so it must not trip the breaker, or one buggy client could take
// a healthy provider out of rotation for everyone.
func (e *Error) CountsAsFailure() bool {
	switch e.Kind {
	case KindBadRequest, KindContentFilter, KindCanceled:
		return false
	default:
		return true
	}
}

// AsError extracts an *Error from err. Errors that are not *Error (bugs,
// unexpected wrapping) come back as KindUnknown so callers always get a kind.
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return &Error{Kind: KindUnknown, Err: err}
}
