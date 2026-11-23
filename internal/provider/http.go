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

// Do runs one attempt; on success the caller owns resp.Body and the Deadline and must Release it.
func Do(hc *http.Client, name string, timeout time.Duration, ctx context.Context,
	build func(ctx context.Context) (*http.Request, error)) (*http.Response, *Deadline, error) {

	dl := NewDeadline(ctx, timeout)
	req, err := build(dl.Ctx)
	if err != nil {
		// Canceled before dial is a transport failure, not BadRequest.
		if ctxErr := dl.Ctx.Err(); ctxErr != nil {
			e := TransportError(name, ctxErr, dl)
			dl.Release()
			return nil, nil, e
		}
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
		_ = resp.Body.Close()
		dl.Release()
		return nil, nil, e
	}
	return resp, dl, nil
}

func StatusError(name string, resp *http.Response) *llm.Error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10)) // drain for conn reuse
	e := &llm.Error{Provider: name, Status: resp.StatusCode, Err: fmt.Errorf("%s", bytes.TrimSpace(body))}
	switch s := resp.StatusCode; {
	case s == 400 || s == 413 || s == 422:
		e.Kind = llm.KindBadRequest
	case s == 401 || s == 403:
		e.Kind = llm.KindAuth
	case s == 404: // not a provider health signal; model just isn't configured here
		e.Kind = llm.KindNotFound
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

// TransportError classifies dial/read failures. Parent context is checked
// before dl.fired so a disconnect racing the deadline counts as client-left.
func TransportError(name string, err error, dl *Deadline) *llm.Error {
	e := &llm.Error{Provider: name, Kind: llm.KindUnavailable, Err: err}
	var classified *llm.Error
	var ne net.Error
	switch {
	case errors.Is(dl.parent.Err(), context.Canceled):
		e.Kind = llm.KindCanceled
	case errors.As(err, &classified):
		e.Kind, e.Status, e.RetryAfter = classified.Kind, classified.Status, classified.RetryAfter
	case dl.fired.Load():
		e.Kind = llm.KindTimeout
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
