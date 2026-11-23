# Progress and handoff

Last updated: 2026-11-19. This file is the starting point for any future
development session: read it, then DECISIONS.md, then the code.

## Status

All code for the 3-week plan is written. Each feature was verified by
**running it** against the mock providers, not by unit tests (new tests were
deliberately skipped to move fast; see "Gaps").

| Area | Status | How it was verified |
|---|---|---|
| OpenAI-compatible API, streaming (SSE) | done | curl, demo.sh |
| Adapters: OpenAI-compatible, Anthropic | done | OpenAI-compatible: live vs Groq + Gemini (2026-11-22, `bench/live-test.sh`). Anthropic: mock only |
| Mock provider (latency, errors, hang, reset, mid-stream abort) | done | used by every test below |
| Router: priority / fastest / cheapest / weighted | done | header override, each picks the expected deployment |
| Sticky routing (`X-Gateway-Session`, D34) | done | 30 conversations × 5 turns: each stayed on one provider; moved on failure, stayed after heal |
| Circuit breaker, retry budget, hedging | done | hang test: breaker opened after 5 failures, recovered via half-open |
| Redis cache (fail open + 5s backoff) | done | hit/miss/skip, Redis-down test |
| API keys, rate limit, spend cap (Redis Lua) | done | 401s, 429 after burst, spend cap blocks |
| Kafka → Postgres usage pipeline | done | 5 events → 5 rows; replay from offset 0 → no duplicates |
| Prometheus + Grafana dashboard | done | target up, dashboard loads |
| Full compose stack on Colima | done | all 10 containers up |
| Kubernetes manifests | written | **never applied to a cluster** |
| Benchmarks: failover | **measured** | bench/results/failover-* |
| Benchmarks: throughput, cache, streaming | scripts written | **not run yet** |

## Measured results (only real numbers so far)

`MODE=<kill|error|hang> RPS=500 ./bench/failover.sh`, mock-a broken for 20s of 60s:

| Fault | Requests | Errors | p99 |
|---|---|---|---|
| kill | 29,996 | 0 | 55.5 ms |
| 503 | 29,996 | 0 | 61.6 ms |
| hang | 29,977 | 0 | 2,051.8 ms (≈2s of slow requests = provider timeout) |

## What's left

1. Commit everything (`git add -A && git commit`), if not already done.
2. Run remaining benchmarks: `./bench/run-baseline.sh` (needs `brew install k6`),
   `./bench/cache.sh`, `./bench/streaming.sh`. Paste results into README.
3. Grafana screenshot for README (`make up`, send traffic, `localhost:3000`).
4. Optional: unit tests for router strategies and breaker state machine.
5. Optional: smoke test with one real provider key (use a gateway-specific env
   var like `LLMGW_ANTHROPIC_API_KEY`, not `ANTHROPIC_API_KEY`, which other
   tools on the machine may also read).
6. Optional: apply k8s manifests on `colima start --kubernetes`.
7. Not built: Gemini adapter, config hot reload, semantic cache.

## Known gaps

- OpenAI-only request params (`seed`, `frequency_penalty`, `presence_penalty`,
  `logprobs`, `response_format`) are **silently ignored**, which contradicts D2
  ("reject loudly"). Fix: reject them with 400 in `internal/api/request.go`.
- `/admin/deployments` stats are per (group, deployment), so the same provider
  appears once per model group.
- Spend cap is soft (can overshoot by in-flight requests) by design (D28).
- Usage events buffered in memory are lost on SIGKILL (D30).

## Environment notes

- Docker runs via Colima: `colima start --cpu 4 --memory 6`.
- Compose uses the Docker Hub Redpanda image, since `docker.redpanda.com` is blocked on some networks.
- go.mod requires Go 1.26 (auto-downloaded toolchain); Dockerfile uses golang:1.26-alpine.
- Benchmarks run natively (not in Colima) to avoid port-forwarding latency.

## Learning the code: reading order

Follow one request through the system. Progress: ✅ = done.

| # | File | Why |
|---|---|---|
| 1 | internal/llm/types.go ✅ | request/response shapes everything uses |
| 2 | internal/llm/errors.go ✅ | error kinds; Retryable vs CountsAsFailure |
| 3 | config.example.yaml | provider, model group, deployment |
| 4 | cmd/gateway/main.go | startup: what's built and wired |
| 5 | internal/api/chat.go — `handleChat` | where a request enters |
| 6 | internal/gateway/gateway.go — `Chat`, `chatUpstream` | attempts, failover, hedging |
| 7 | internal/router/router.go — `Plan` | strategy ranking |
| 8 | internal/router/breaker.go | circuit breaker state machine |
| 9 | internal/provider/openai/openai.go — `Chat` | the actual HTTP call |
| 10 | internal/provider/http.go | HTTP errors → error kinds |

After #10 the main flow is understood; cache, auth, usage, metrics are add-ons.
