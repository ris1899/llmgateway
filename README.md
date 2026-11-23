# llmgateway

An LLM gateway in Go. One OpenAI-compatible API in front of multiple
providers (OpenAI, Anthropic, any OpenAI-compatible endpoint). Headline
feature: cost- and latency-aware routing with measured failover.

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

A model group is a public model name backed by interchangeable deployments
(provider + upstream model + price). Strategy is the group default,
overridable per API key or per request with `X-Gateway-Strategy`:

| Strategy | Picks | Uses |
|---|---|---|
| `priority` | configured order, fail over down the list | none |
| `fastest` | lowest predicted latency | base-latency p95 + expected tokens × time-per-token, inflated by error rate |
| `cheapest` | cheapest deployment predicted to meet `X-Gateway-Latency-Target-Ms` | price per token × estimated tokens, plus the latency prediction |
| `weighted` | random split by weight, rest as fallbacks | canaries, A/B |

Diagrams and request flow: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Design choices

- Failover only before the first streamed token. After that the client has
  seen output, so a provider dying mid-stream ends the stream with an error.
- One circuit breaker per (model group, deployment). It trips on consecutive
  failures or on error rate, and probes one request at a time to recover.
- Retries and hedges are capped at about 20% of traffic, so a slow provider
  can't trigger a retry storm. Failing over away from an open breaker is free.
- Only the provider's faults count against its health: a 400 or a 404 for a
  misconfigured model fails over (404) or fails fast (400) without tripping
  the breaker.
- Redis-backed features (cache, rate limits, sessions) fail open: if Redis is
  down, requests are still served.
- Usage billing is exactly-once in effect: events are deduped on `event_id`
  in Postgres, and Kafka offsets are committed only after the transaction.

## Benchmarks

Scripted runs on one Apple Silicon laptop, gateway/mocks/loadgen all native.
Raw results in `bench/results/`; mocks have fixed 50 ms latency, so these
numbers are about gateway overhead, not real-provider performance.

### Failover: mock-a broken for 20 s of a 60 s run at 500 rps (`MODE=<kill|error|hang> RPS=500 ./bench/failover.sh`)

| Fault on primary | Requests | Errors | p99 | Degraded seconds | Back on primary after restore |
|---|---|---|---|---|---|
| Process killed (connection refused) | 29,996 | 0 | 55.5 ms | 0 | 2 s |
| Every request returns 503 | 29,996 | 0 | 61.6 ms | 0 | 0 s |
| Hangs (never answers) | 29,977 | 0 | 2,051.8 ms | 2 | 3 s |

Zero client-visible errors in all three modes; a hang costs the full
provider timeout until the breaker opens, so that timeout is the knob that
matters.

### Verified against real providers (`./bench/live-test.sh`, free tiers, ~20 requests, 2026-11-22)

| Check | Result |
|---|---|
| Non-streaming call, each provider | 200 from both (Groq 786 ms, Gemini 2,562 ms upstream) |
| SSE streaming (Groq) | 15 `data:` events, ended with `[DONE]` |
| Failover: Gemini made unreachable (1 ms timeout), Groq as fallback | 7 of 7 requests returned 200 from Groq, `Attempts: 2`; Gemini's breaker ended `open` |
| `fastest` strategy | Groq for 5 of 6 requests (measured p50 747 ms); 6th went to Gemini (too few samples to rank yet) |

Failover between two real providers is invisible to the client (570-672 ms
vs 616-952 ms direct, same run). Not covered: Anthropic adapter (no key),
`cheapest` (free tier, prices are 0), load/throughput.

## API

| Endpoint | |
|---|---|
| `POST /v1/chat/completions` | OpenAI format; `stream: true` for SSE. Works with OpenAI SDKs via base URL. |
| `GET /v1/models` | configured model groups |
| `GET /healthz`, `GET /readyz` | liveness vs readiness |
| `:8081/metrics`, `:8081/admin/deployments`, `:8081/debug/pprof` | internal port only |

Request headers: `Authorization: Bearer <key>`, `X-Gateway-Strategy`,
`X-Gateway-Latency-Target-Ms`, `X-Gateway-No-Cache: true`,
`X-Gateway-Session: <conversation id>` (sticky routing: keeps a conversation
on one provider so its prompt cache is reused).

Response headers: `X-Gateway-Provider`, `-Attempts`, `-Upstream-Ms`,
`-Overhead-Ms`, `-Cost-Usd`, `-Cache` (hit/miss/skip), `-Hedged`, `-Sticky`
(new/hit/moved), also set on error responses where at least one deployment
was tried.

Unknown request body fields get 400 `unsupported parameter: <name>`, not
silently dropped (`seed`, `frequency_penalty`, `logit_bias`,
`response_format`, etc.); `tools`/`functions`, images and `n > 1` are
rejected the same way (text-only).

## Development

```bash
make test    # go test -race ./...
make lint    # golangci-lint run
make cover   # coverage profile + summary
make ci      # fmt-check, vet, build, test, lint -- what CI runs
```

Unit/e2e tests cover router, breaker, gateway pipeline, API/SSE, request
validation, provider adapters, auth. Anything needing real Redis/Kafka/
Postgres (the rate-limit Lua script, cache round trips, the usage consumer)
was verified manually.

## Known limitations

- A hung provider can't be detected faster than its timeout.
- Spend caps are soft: in-flight requests can overshoot, and a request that
  spans a UTC month boundary can be billed to the next month.
- Usage events still buffered in memory are lost on `SIGKILL`.
- Text only: tools, images and `n > 1` are rejected.
- Config is read at startup; changes need a restart.
- The Anthropic adapter is tested against a mock, not the real API.
- Kubernetes manifests in `deploy/k8s` have never been applied to a cluster.
- Not built: native Gemini adapter, config hot reload, semantic caching.

## Real providers

Copy `config.real.example.yaml` to `config.local.yaml` (gitignored). Fill in
model ids and current prices, export API keys as env vars; never stored in
config.
