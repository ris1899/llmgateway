package llm

import (
	"errors"
	"fmt"
	"time"
)

// ErrorKind classifies upstream failures into provider-agnostic buckets.
type ErrorKind int

const (
	KindUnknown ErrorKind = iota
	KindTimeout
	KindUnavailable
	KindRateLimited // 429 from the provider, not our own limiter
	KindBadRequest
	KindAuth
	KindContentFilter
	KindCanceled // our side gave up; says nothing about the provider
	KindNotFound // this provider doesn't serve the requested model
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
	case KindNotFound:
		return "not_found"
	default:
		return "unknown"
	}
}

type Error struct {
	Provider   string
	Kind       ErrorKind
	Status     int // 0 if none, e.g. dial error
	RetryAfter time.Duration
	Err        error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %s (status %d): %v", e.Provider, e.Kind, e.Status, e.Err)
}

func (e *Error) Unwrap() error { return e.Err }

// Retryable reports whether another provider could plausibly succeed.
// CountsAsFailure reports whether this reflects badly on this provider's health.
//
//	Kind           Retryable  CountsAsFailure
//	Timeout        yes        yes
//	Unavailable    yes        yes
//	RateLimited    yes        yes
//	Auth           yes        yes
//	Unknown        yes        yes
//	NotFound       yes        no
//	BadRequest     no         no
//	ContentFilter  no         no
//	Canceled       no         no
func (e *Error) Retryable() bool {
	switch e.Kind {
	case KindBadRequest, KindContentFilter, KindCanceled:
		return false
	default:
		return true
	}
}

func (e *Error) CountsAsFailure() bool {
	switch e.Kind {
	case KindBadRequest, KindContentFilter, KindCanceled, KindNotFound:
		return false
	default:
		return true
	}
}

// AsError extracts an *Error from err, defaulting to KindUnknown.
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return &Error{Kind: KindUnknown, Err: err}
}
