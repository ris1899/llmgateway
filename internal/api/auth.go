package api

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"llmgateway/internal/auth"
	"llmgateway/internal/ratelimit"
)

// withAuth authenticates the API key, then applies its rate limit and spend
// cap. Order: auth first (cheap, cached), then the Redis round trip.
//
// Failure policy when Redis is down: fail OPEN (allow). The trade-off: during
// a Redis outage, limits aren't enforced, but the gateway keeps serving. The
// alternative, fail closed, turns a Redis outage into a full outage for every
// customer. For a gateway whose job is availability, open is the right
// default; the error counter makes the outage visible.
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
				s.m.RateLimited.WithLabelValues("error").Inc() // fail open, see above
			case dec == ratelimit.RateLimited:
				s.m.RateLimited.WithLabelValues("rate").Inc()
				w.Header().Set("Retry-After", strconv.Itoa(max(1, int(retry.Seconds()+0.999))))
				writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "rate limit exceeded for this API key")
				return
			case dec == ratelimit.OverSpendCap:
				s.m.RateLimited.WithLabelValues("spend").Inc()
				// 402 would be semantically neat, but OpenAI SDKs treat 429 with
				// type "insufficient_quota" as the quota signal, so mirror that.
				writeError(w, http.StatusTooManyRequests, "insufficient_quota", "monthly spend cap reached for this API key")
				return
			}
		}
		next.ServeHTTP(w, r.WithContext(auth.WithKey(r.Context(), key)))
	})
}
