package provider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"llmgateway/internal/llm"
)

// Do runs one upstream attempt with a Deadline and turns every failure into a
// classified *llm.Error. On success the caller owns resp.Body and the Deadline
// and must Release it.
func Do(hc *http.Client, name string, timeout time.Duration, ctx context.Context,
	build func(ctx context.Context) (*http.Request, error)) (*http.Response, *Deadline, error) {

	dl := NewDeadline(ctx, timeout)
	req, err := build(dl.Ctx)
	if err != nil {
		dl.Release()
		return nil, nil, &llm.Error{Provider: name, Kind: llm.KindBadRequest, Err: err}
	}
	resp, err := hc.Do(req)
	if err != nil {
		e := TransportError(name, err, dl)
		dl.Release()
		return nil, nil, e
	}
	if resp.StatusCode != http.StatusOK {
		e := StatusError(name, resp)
		resp.Body.Close()
		dl.Release()
		return nil, nil, e
	}
	return resp, dl, nil
}

// StatusError maps a non-200 upstream response to an error kind.
func StatusError(name string, resp *http.Response) *llm.Error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	e := &llm.Error{Provider: name, Status: resp.StatusCode, Err: fmt.Errorf("%s", bytes.TrimSpace(body))}
	switch s := resp.StatusCode; {
	case s == 400 || s == 413 || s == 422:
		e.Kind = llm.KindBadRequest
	case s == 401 || s == 403:
		e.Kind = llm.KindAuth
	case s == 404:
		// Usually our config names a model this provider doesn't have. Another
		// deployment may still serve it, so treat as retryable, not BadRequest.
		e.Kind = llm.KindUnavailable
	case s == 408:
		e.Kind = llm.KindTimeout
	case s == 429:
		e.Kind = llm.KindRateLimited
		e.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
	case s >= 500: // includes Anthropic's 529 "overloaded"
		e.Kind = llm.KindUnavailable
	default:
		e.Kind = llm.KindUnknown
	}
	return e
}

// TransportError classifies dial/read failures. Order matters: check *why* the
// context ended before looking at the raw error, because a canceled request
// surfaces as a generic read error.
func TransportError(name string, err error, dl *Deadline) *llm.Error {
	e := &llm.Error{Provider: name, Kind: llm.KindUnavailable, Err: err}
	var ne net.Error
	switch {
	case dl.fired.Load():
		e.Kind = llm.KindTimeout
	case errors.Is(dl.parent.Err(), context.Canceled):
		e.Kind = llm.KindCanceled
	case errors.Is(dl.parent.Err(), context.DeadlineExceeded):
		e.Kind = llm.KindTimeout
	case errors.As(err, &ne) && ne.Timeout():
		e.Kind = llm.KindTimeout
	}
	return e
}

func parseRetryAfter(v string) time.Duration {
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(0, time.Until(t))
	}
	return 0
}
