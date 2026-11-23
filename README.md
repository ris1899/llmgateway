# llmgateway

An LLM gateway in Go. One OpenAI-compatible API in front of multiple providers
(OpenAI, Anthropic, any OpenAI-compatible endpoint), with **cost- and
latency-aware routing and measured failover** as the headline feature.

```
            ┌──────────────────────────── llmgateway ────────────────────────────┐
 client ──► │ request id → auth (keys) → rate limit + spend cap ──┐               │
 (OpenAI    │                              (Redis Lua, atomic)    ▼               │
  SDK/curl) │                                             cache lookup (Redis)    │
            │                                                     │ miss          │
            │   router: strategy (priority / fastest / cheapest / weighted)       │
            │     uses live stats per deployment: base latency p95, time/token,   │
            │     error rate, circuit breaker state                               │
            │                                                     ▼               │
            │   attempts: breaker gate → retry budget → optional hedge ──────────►│──► OpenAI-compatible
            │   SSE streaming, failover until first token                         │──► Anthropic
            │                                                     │               │──► mock providers
            │   usage event ─► Kafka (Redpanda) ─► consumer ─► Postgres           │
            │   metrics ─► Prometheus ─► Grafana                                   │
            └─────────────────────────────────────────────────────────────────────┘
```

## Quick start (no API keys, no dependencies)

Requires Go 1.26+.

```bash
make mocks     # 3 mock providers on :9001-9003
make run       # gateway on :8080, admin/metrics on :8081
```

```bash
curl -s localhost:8080/v1/chat/completions \
  -d '{"model":"mock-chat","messages":[{"role":"user","content":"hi"}]}' | jq
./bench/demo.sh   # two providers, streaming, live failover
make stop
```

## Full stack (Colima)

Gateway, mocks, Redis, Redpanda (Kafka), Postgres, usage consumer, Prometheus, Grafana:

```bash
colima start --cpu 4 --memory 6
make up                                  # docker compose -f deploy/compose.yaml up --build -d
export DATABASE_URL='postgres://llmgw:llmgw@localhost:5432/llmgw?sslmode=disable'
go run ./cmd/keyctl create -name demo -rps 10 -burst 20 -cap 5   # prints a key once
curl -s -H "Authorization: Bearer <key>" localhost:8080/v1/chat/completions \
  -d '{"model":"mock-chat","messages":[{"role":"user","content":"hi"}]}'
open http://localhost:3000               # Grafana dashboard "LLM Gateway"
make down
```

## Routing

A **model group** is a public model name backed by interchangeable deployments
(provider + upstream model + price). Strategy is the group default, overridable
per API key, overridable per request with `X-Gateway-Strategy`:

| Strategy | Picks | Uses |
|---|---|---|
| `priority` | configured order, fail over down the list | — |
| `fastest` | lowest predicted latency | base-latency p95 + expected tokens × time-per-token, inflated by error rate |
| `cheapest` | cheapest deployment predicted to meet `X-Gateway-Latency-Target-Ms` | price per token × estimated tokens, plus the latency prediction |
| `weighted` | random split by weight, rest as fallbacks | canaries, A/B |

Reliability, per deployment: circuit breaker (consecutive failures or error
rate, half-open probes), "no progress" timeout, retry budget (retries capped at
~20% of traffic, so they can't cause a storm), optional hedged requests.
How it all fits together, with diagrams: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
Why each choice was made: [DECISIONS.md](DECISIONS.md).

## Benchmarks

All numbers below come from scripted runs on one machine. Raw results are in
`bench/results/`. Reproduce with the commands shown.

**Setup:** Apple Silicon Mac laptop. Gateway, mocks and load generator all
run natively on the same machine (not in Colima), so they compete for CPU.
Mock providers have a fixed 50 ms latency. Open-model load (constant arrival
rate). These numbers come from mocks, not real providers.

### Failover: mock-a broken for 20 s of a 60 s run at 500 rps

`MODE=<kill|error|hang> RPS=500 ./bench/failover.sh`

| Fault on primary | Requests | Errors | p99 | Degraded seconds | Back on primary after restore |
|---|---|---|---|---|---|
| Process killed (connection refused) | 29,996 | 0 | 55.5 ms | 0 | 2 s |
| Every request returns 503 | 29,996 | 0 | 61.6 ms | 0 | 0 s |
| Hangs (never answers) | 29,977 | 0 | 2,051.8 ms | 2 | 3 s |

What this shows:
- **Zero client-visible errors** in all three modes.
- When the provider fails fast (killed, or returning 503), failover costs nothing
  noticeable: the failed attempt returns in about 1 ms and the circuit breaker
  opens after 5 consecutive failures.
- **A hang is different:** no error ever comes back, so the first requests each
  wait the full provider timeout (2 s in this config) before failing over.
  For about 2 seconds, p99 equals the timeout. Then the breaker opens and
  latency returns to normal. **For hangs, recovery time ≈ the provider timeout**,
  so that timeout is the knob that matters.
- Before the circuit breaker existed, *every* request paid the full timeout for
  as long as the provider hung.

### Verified against real providers

`./bench/live-test.sh` (config in `config.local.yaml`, keys from env vars).
One run on 2026-11-22 against **Groq** (`openai/gpt-oss-20b`) and **Google
Gemini** (`gemini-flash-lite-latest`), free tiers, about 20 requests. This
checks correctness, not performance. Raw output:
`bench/results/live-20261122-133628.txt`.

| Check | Result |
|---|---|
| Non-streaming call, each provider | 200 from both (Groq 786 ms, Gemini 2,562 ms upstream) |
| SSE streaming (Groq) | 15 `data:` events, ended with `[DONE]` |
| Failover: Gemini made unreachable (1 ms timeout), Groq as fallback | **7 of 7 requests returned 200** from Groq, `Attempts: 2`; Gemini's breaker ended `open` |
| `fastest` strategy | Groq for 5 of 6 requests (measured p50 747 ms); the 6th went to Gemini because it had too few samples to rank yet (new deployments are tried optimistically) |

What this shows and what it doesn't:
- The OpenAI-compatible adapter works against two real APIs, including streaming.
- Failover between two real providers is invisible to the client. Failover
  requests took 570–672 ms upstream, within the range of direct Groq calls
  (616–952 ms) in the same run; with this few requests, Groq's own variance
  dominates.
- In this run every failover request showed `Attempts: 2`, because requests
  were spaced longer than the breaker's 2 s cooldown, so each one was a
  half-open probe. The script now sends them back to back.
- **Not covered:** the Anthropic adapter (no key; tested against a mock only),
  `cheapest` (free tier, so all prices are 0), and any load or throughput.

### Not yet measured (run these to fill)

| Measurement | Command |
|---|---|
| Throughput and latency at 500..5000 rps, gateway vs direct | `./bench/run-baseline.sh` (needs `brew install k6`) |
| Cache hit vs miss latency | `./bench/cache.sh` (needs Redis on :6379) |
| Streaming TTFT overhead, and 10k concurrent streams | `./bench/streaming.sh` (raise `ulimit -n` first; see bench/README.md) |
| Grafana screenshot | `make up`, generate load, screenshot `http://localhost:3000` |

What's already been seen informally, not benchmarked: per-request gateway
overhead (`X-Gateway-Overhead-Ms`) stayed around 0.1 ms at p99 during the
failover runs. Note that header is measured before the response is encoded.

## API

| Endpoint | |
|---|---|
| `POST /v1/chat/completions` | OpenAI format; `stream: true` for SSE. Works with OpenAI SDKs via base URL. |
| `GET /v1/models` | configured model groups |
| `GET /healthz`, `GET /readyz` | liveness vs readiness |
| `:8081/metrics`, `:8081/admin/deployments`, `:8081/debug/pprof` | internal port only |

Request headers: `Authorization: Bearer <key>` (when auth is on),
`X-Gateway-Strategy`, `X-Gateway-Latency-Target-Ms`, `X-Gateway-No-Cache: true`,
`X-Gateway-Session: <conversation id>` (sticky routing: keeps a conversation on
one provider so its prompt cache is reused, D34).

Response headers: `X-Gateway-Provider`, `-Attempts`, `-Upstream-Ms`,
`-Overhead-Ms`, `-Cost-Usd`, `-Cache` (hit/miss/skip), `-Hedged`, `-Sticky` (new/hit/moved).

Not supported (rejected with 400, never silently dropped): tools/function
calling, images, `n > 1`.

## Breaking a provider live

```bash
curl -X PATCH localhost:9001/admin/behavior -d '{"error_rate":1,"error_status":503}'
curl -X PATCH localhost:9001/admin/behavior -d '{"fault":"hang"}'
curl -X PATCH localhost:9001/admin/behavior -d '{"fault":"reset"}'
curl -X PATCH localhost:9001/admin/behavior -d '{"fail_after_tokens":3}'   # die mid-stream
curl -X PATCH localhost:9001/admin/behavior -d '{"fault":"","error_rate":0,"fail_after_tokens":0}'
curl -s localhost:8081/admin/deployments | jq    # breaker state, live stats
```

## Real providers

Copy `config.real.example.yaml` to `config.local.yaml` (gitignored). Fill in
model ids and current prices from each provider's pricing page, and export the
API keys as env vars. Keys are never stored in config.

## Kubernetes

`deploy/k8s/`: Deployment (2 replicas, readiness/liveness, drain delay),
Service, PDB, HPA, mocks, Redis. See [deploy/k8s/README.md](deploy/k8s/README.md)
for running it on Colima's built-in k3s.

## Layout

```
cmd/gateway         main + wiring config to objects
cmd/mockprovider    fake provider (OpenAI + Anthropic wire formats)
cmd/keyctl          create/list/revoke API keys
cmd/usageconsumer   Kafka → Postgres aggregation
internal/llm        canonical types and error kinds (leaf package)
internal/config     YAML loading + validation
internal/provider   Provider interface, HTTP client, deadlines, SSE reader; openai/, anthropic/
internal/router     stats, circuit breaker, retry budget, strategies
internal/gateway    pipeline: cache → plan → attempts (failover, hedging) → record
internal/api        HTTP handlers, SSE, auth/limit middleware
internal/cache      Redis exact-match cache
internal/ratelimit  token bucket + spend cap (Lua)
internal/auth       hashed API keys
internal/usage      Kafka producer, consumer
internal/metrics    all Prometheus metrics
migrations/         Postgres schema
deploy/             Dockerfile, compose, Grafana, Prometheus, k8s
bench/              loadgen, k6, failover/cache/streaming scripts, results/
```
