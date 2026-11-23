package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"llmgateway/internal/auth"
	"llmgateway/internal/ratelimit"
)

// Fails open on a Redis error: limits go unenforced rather than the gateway
// going fully down; the error counter makes the outage visible.
func (s *Server) withAuth(next http.Handler) http.Handler {
	if s.auth == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		plain, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || plain == "" {
			writeError(w, http.StatusUnauthorized, "invalid_api_key", "missing API key (Authorization: Bearer ...)")
			return
		}
		key, err := s.auth.Lookup(r.Context(), plain)
		switch {
		case errors.Is(err, auth.ErrInvalidKey):
			writeError(w, http.StatusUnauthorized, "invalid_api_key", "invalid API key")
			return
		case err != nil:
			s.log.Error("key lookup failed", "request_id", RequestID(r.Context()), "err", err)
			writeError(w, http.StatusServiceUnavailable, "server_error", "authentication unavailable")
			return
		}

		if s.limiter != nil {
			dec, retry, err := s.limiter.Check(r.Context(), key.ID, ratelimit.Limits{
				RPS: key.RPS, Burst: key.Burst, MonthlyCapUSD: key.MonthlyCapUSD})
			switch {
			case err != nil:
				s.m.RateLimited.WithLabelValues("error").Inc()
			case dec == ratelimit.RateLimited:
				s.m.RateLimited.WithLabelValues("rate").Inc()
				w.Header().Set("Retry-After", strconv.Itoa(max(1, int(retry.Seconds()+0.999))))
				writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "rate limit exceeded for this API key")
				return
			case dec == ratelimit.OverSpendCap:
				s.m.RateLimited.WithLabelValues("spend").Inc()
				// mirrors OpenAI SDKs' quota signal (429 insufficient_quota) rather than 402
				writeError(w, http.StatusTooManyRequests, "insufficient_quota", "monthly spend cap reached for this API key")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(auth.WithKey(r.Context(), key)))
	})
}
