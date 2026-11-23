# Design Decisions

Each entry: what we chose, what else we considered, and why. Written for
interview prep: the "why" matters more than the "what".

---

## D1. Module layout: `internal/llm` as a leaf package

**Chose:** canonical request/response/error types live in `internal/llm`, which
imports nothing else internal.
**Alternatives:** types inside `provider/` or `api/`.
**Why:** `api`, `router`, `cache`, `provider` and `usage` all need the request
type. If it lived in `provider`, then `router → provider` and `provider → router`
(for health callbacks) would cycle. A leaf package makes the dependency graph a
tree. Go forbids import cycles, so this is structural, not stylistic.

## D2. Canonical format is OpenAI-shaped, text-only in v1

**Chose:** public API is OpenAI-compatible; internal types mirror it. Tools,
images and `n>1` are rejected with a 400.
**Alternatives:** a provider-neutral superset format; silently dropping
unsupported fields.
**Why:** every OpenAI SDK works against us with only a base URL change. Tool
calling differs enough across OpenAI/Anthropic/Gemini that normalizing it is a
project of its own. Rejecting loudly beats dropping silently: a client that sent
tools and got a plain text answer would have a confusing bug.

## D3. Optional sampling params are pointers

**Chose:** `Temperature *float64` etc.
**Why:** "unset" and "0" mean different things. Unset = provider default (1.0
for OpenAI, non-deterministic), 0 = near-deterministic. The cache can only
safely store the latter, so it needs to tell them apart.

## D4. Errors classified into kinds at the adapter boundary

**Chose:** adapters return `*llm.Error{Kind}`; router logic only reads `Kind`.
`Retryable()` and `CountsAsFailure()` are separate questions.
**Alternatives:** router inspects HTTP status codes directly.
**Why:** provider-agnostic failover. And the two questions differ: a 400 is not
retryable (every provider would reject it) and must not count against the
provider's health, otherwise one buggy client can open the circuit breaker
and take a healthy provider away from everyone.

## D5. Config: YAML, strict, secrets via env, all errors reported at once

**Chose:** `KnownFields(true)`, `api_key_env` indirection, `errors.Join` of
every validation problem, missing key = startup failure.
**Alternatives:** env-only config (12-factor), JSON, Viper.
**Why:** routing policy and price tables are nested data that env vars express
badly. Strict parsing turns typos into startup errors instead of silent
defaults. Secrets stay out of the file so it's safe to commit. Viper is a large
dependency for what is ~200 lines here. Failing on a missing key (vs. warning)
because a provider that 401s on every call pollutes health stats.

## D6. stdlib `net/http` ServeMux, no router library

**Chose:** Go 1.22+ pattern routing (`"GET /healthz"`).
**Alternatives:** chi, gin, echo.
**Why:** we have ~5 routes. The stdlib now does method matching and path
params. Fewer dependencies, and gin's custom context would get in the way of
`http.ResponseController` for SSE flushing.

## D7. No `http.Server.WriteTimeout`

**Why:** it caps the *entire* response duration, so it would cut off long SSE
streams. Slowloris protection comes from `ReadHeaderTimeout`; request duration
is bounded by per-request context deadlines instead.

## D8. Liveness vs readiness

**Chose:** `/healthz` always 200 while the process runs; `/readyz` reflects
dependency state and flips false at shutdown start.
**Why:** if liveness checked Redis, a Redis blip would make Kubernetes restart
every gateway pod at the same moment, turning a dependency hiccup into a full
outage. Readiness failing just removes the pod from the load balancer.

## D9. Graceful shutdown order

**Chose:** SIGTERM → readiness false → drain delay (`GATEWAY_DRAIN_DELAY`) →
`Shutdown()` with timeout.
**Why:** in Kubernetes, endpoint removal propagates asynchronously, so traffic
keeps arriving for a few seconds after SIGTERM. Closing the listener
immediately refuses those requests. Delay is 0 locally so Ctrl-C stays fast.

## D10. Middleware writer must implement `Unwrap()`

**Why:** `http.ResponseController` follows `Unwrap` to find the real `Flusher`.
A wrapping writer without it silently breaks SSE: chunks buffer until the
response ends, and time to first token becomes time to last token.

## D11. Mock provider speaks real wire formats, controllable at runtime

**Chose:** a mock that implements OpenAI and Anthropic formats (including SSE
event shapes and Anthropic's required `max_tokens`) with an admin `PATCH`
endpoint for latency, errors, hangs, TCP resets and mid-stream aborts.
**Alternatives:** Go interface fakes in tests only; WireMock-style recorded responses.
**Why:** interface fakes skip the adapters, HTTP client and SSE parsing, which is
exactly where real bugs live. A real server exercises the whole path, makes
benchmarks repeatable and free, and runtime control enables "break it live"
demos without restarts. Behavior is held in an `atomic.Pointer` so the hot path
never takes a lock.

## D12. One adapter for every OpenAI-compatible API

**Why:** OpenRouter, Groq, vLLM, Ollama and our mock all speak OpenAI's format;
only base URL and key differ. Fewer adapters = less code to keep correct.

## D13. Pull-based Stream interface (`Recv()`), not channels

**Alternatives:** adapter goroutine pushing chunks into a `chan`.
**Why:** a channel needs a goroutine per stream and careful shutdown; if the
client disconnects and nobody drains the channel, the goroutine leaks. With
`Recv()`, the HTTP handler reads upstream only as fast as it writes downstream
(natural backpressure), and closing the stream closes the upstream body.
Same shape as gRPC client streams.

## D14. Shared HTTP client with a large idle pool, no `Client.Timeout`

**Why:** Go's default transport keeps 2 idle connections per host. At thousands
of rps nearly every call would open a new TCP/TLS connection and exhaust
ephemeral ports in TIME_WAIT. `MaxIdleConnsPerHost=1024` keeps connections hot.
`Client.Timeout` is avoided because it caps the whole body read and would kill
long streams.

## D15. Per-attempt "no progress" deadline instead of a fixed timeout

**Chose:** a timer that covers time to first byte and resets on every streamed
event (`provider.Deadline`).
**Alternatives:** `context.WithTimeout(60s)` per attempt.
**Why:** a fixed cap either kills healthy long streams or is too loose to catch
a hung provider. "No bytes for N seconds" catches hangs without capping length.
The deadline also records whether *it* fired, which lets us distinguish
"provider timed out" (provider's fault, counts against health) from "client
disconnected" (`KindCanceled`, must not count, or clients closing tabs would
trip circuit breakers).

## D16. Streaming: commit to a provider only after its first token

**Chose:** `gateway.ChatStream` reads the first real chunk before the API layer
writes response headers.
**Why:** until headers are sent we can still fail over silently and return a
proper status code. Many provider failures (overloaded, 5xx, hang) happen
before the first token, so this covers most failures for the cost of zero
extra latency (we'd wait for that token anyway).

## D17. Mid-stream failure: error event, no resume

**Chose:** after the first token, an upstream failure produces an SSE
`{"error":...}` event and the stream ends **without** `[DONE]`.
**Alternatives:** resume on another provider by replaying the partial answer
as context.
**Why:** a resumed answer is stitched from two models and can contradict
itself; billing is also doubled. Omitting `[DONE]` lets clients distinguish a
cut stream from a complete one. Revisit if a real client needs resume.

## D18. Errors sanitized at the API edge

**Why:** raw upstream errors include internal hostnames and provider bodies
can echo prompt content. Clients get `upstream <name> failed: <kind>`; full
details go to logs keyed by request id. A provider 401 becomes a 502: it means
*our* provider key is broken, not the caller's.

## D19. Gateway overhead measured in-process and returned as a header

**Chose:** `X-Gateway-Overhead-Ms = total handler time - upstream time`.
**Alternatives:** only compare "via gateway" vs "direct" latency externally.
**Why:** subtracting two noisy distributions gives a noisy answer, especially at
p99. The in-process number isolates exactly the gateway's own work. We report
both; the external comparison catches costs the internal one can't see
(kernel, connection handling, response encoding).

## D20. Benchmarks use an open model (constant arrival rate)

**Why:** a fixed number of looping clients sends *fewer* requests when the
server slows down, so latency looks better than reality ("coordinated
omission"). k6's `constant-arrival-rate` keeps the offered load fixed.

## D21. Router returns an ordered plan, strategies are pure ranking functions

**Why:** failover becomes "walk the list", and each strategy is a sort over a
snapshot of stats: deterministic and easy to reason about. The router never
does I/O; the gateway executes the plan.

## D22. Latency model: base p95 + expected tokens × time-per-token

**Alternatives:** rank by plain request latency p95.
**Why:** LLM latency is dominated by output length. Plain latency punishes a
provider that happens to get long-answer traffic. Splitting into base latency
(TTFT / prefill + network) and per-token speed predicts *this* request's
latency. Expected tokens = group's EWMA of output length, capped by max_tokens.
Score is inflated by `1/(1-error_rate)` = expected attempts, so a fast but flaky
provider ranks honestly.

## D23. Exploration in fastest/cheapest (2%)

**Why:** a deployment that looks slow never gets traffic again, so its stats
never update, even after it recovers. A small random share keeps every
deployment's data fresh. Unknown latency also ranks first (optimistic), so new
or recovered deployments get sampled quickly.

## D24. Circuit breaker: consecutive failures OR error rate; one probe at a time

**Why two triggers:** consecutive failures catch a hard-down provider within N
requests regardless of traffic; error rate catches a provider failing 60% of
calls that rarely fails 5 in a row. Half-open admits one probe at a time and
needs several successes, so a still-dead provider costs one request per probe
and a flapping one doesn't immediately get full traffic. Per (group,
deployment), not per provider: simpler, and one group's traffic can't flip
another's breaker. Trade-off: a dead provider is detected separately per group.

**Measured (bench/results):** with a hung primary, the breaker limited impact to
~2 seconds of slow requests (≈ provider timeout) instead of every request for
the whole outage. Fast failures (kill, 503) caused no visible degradation.

## D25. Retry budget; failover away from an OPEN breaker is free

**Problem:** "no dropped requests" wants every failed request retried; "no
retry storm" wants retries capped. They conflict during broad degradation.
**Chose:** a Finagle/Envoy-style budget (retries ≤ 20% of requests + 10/s
floor). Failover is *not* charged when the failed deployment's breaker is now
open: the breaker already caps load on the dead provider, and those requests
would go to the healthy one anyway. So a single dead provider gets 100%
failover; a degradation across all providers can't triple load.

## D26. Hedged requests: opt-in, non-streaming only, budgeted

**Why opt-in:** a hedge can bill twice (cancelling the loser may still bill
input tokens). It trades money for tail latency, so it's a per-group config
(`hedge_after`), not a default. Not for streams: two parallel streams double
the cost of the entire answer. Hedges spend retry budget, and a cancelled
loser is recorded as neutral, not as a failure, so hedging can't trip breakers
on healthy providers. A losing half-open probe is still drained and recorded,
otherwise the breaker could stay half-open forever (bug found and fixed while
building).

## D27. Cache: exact match, explicit low temperature only, fail open with backoff

**Key:** SHA-256 of a canonical struct (group, messages, sampling params),
not the raw body, so field order and irrelevant fields don't split keys. The
group, not the deployment, is in the key: any member's answer is valid.
**Only explicit temperature ≤ max:** unset temperature means the provider
default (1.0 for OpenAI), i.e. the client expects variation.
**Fail open with 5s backoff:** first measured without backoff, a Redis outage
added ~100 ms to every request (50 ms timeout × get + set). With backoff,
only the first request after an error pays; the rest skip the cache.
**Streams:** hits are replayed as a stream; stream misses don't populate the
cache (would need to buffer the whole answer).

## D28. Rate limit + spend cap in one Redis Lua script

**Why Lua:** read-refill-decrement must be atomic across gateway replicas;
Redis runs scripts without interleaving. One round trip checks both rate and
spend. **Redis TIME** instead of gateway clocks: replicas' clocks drift.
**Spend in integer nano-USD** (`INCRBY`), not `INCRBYFLOAT`: float sums drift
over millions of tiny additions.
**Soft spend cap:** checked before the request, charged after, so concurrent
in-flight requests can overshoot (observed: $0.0000246 on a $0.00002 cap with
2 requests). A hard cap would reserve worst-case cost up front and reject
legitimate requests near the limit.
**Fail open** when Redis is down: limits pause, the gateway keeps serving.
Fail closed would turn a Redis outage into a total outage.

## D29. API keys: SHA-256, in-memory cache with negative caching

**Why not bcrypt:** keys are 32 random bytes, so brute force is infeasible
regardless of hash speed; bcrypt would add tens of ms per request. Lookups
cached for 30s: Postgres sees one query per key per 30s, not per request.
Trade-off: revocation takes up to 30s. Unknown keys are cached too, so
hammering with a bad key can't hammer Postgres. If Postgres is down, a stale
cached key keeps working (availability over freshness).

## D30. Usage pipeline: at-least-once into Kafka, effectively-once into Postgres

**Producer:** acks=all, idempotent, async with a bounded buffer. If Kafka is
down long enough to fill it, events are dropped and counted, not blocking
requests: billing telemetry must not take down serving. Graceful shutdown
flushes. SIGKILL loses the buffer (zero-loss there needs a local WAL; not done).
**Consumer:** one Postgres transaction per batch: COPY into a temp table,
`INSERT … ON CONFLICT (event_id) DO NOTHING RETURNING *`, aggregate only the
newly inserted rows into `usage_daily`, commit, *then* commit Kafka offsets.
Crash anywhere → redelivery → duplicates are no-ops.
**Verified:** rewound the consumer group to offset 0 and replayed every event:
consumer logged `received 5, new 0`, aggregates unchanged.
**Separate binary:** scales independently; a Postgres outage only stalls the
consumer while Kafka holds the backlog.

## D31. Metrics: bounded labels only

**Why:** each label value is a new time series. Labels are model group,
deployment, status. Never key id or request id: 10k keys × every metric would
blow up Prometheus. Per-key data lives in Postgres via the usage pipeline.
Metrics/admin/pprof on a separate port (:8081) so they're never exposed
through the public load balancer.

## D32. Infrastructure dependencies are optional

**Why:** `make run` needs nothing but Go; benchmarks can isolate one component
(e.g. cache only, via config.bench.yaml); a missing section means the feature
is off rather than a startup failure.

## D33. Failover benchmark measures latency, not just errors

**Why:** the first version of the script reported "0 s recovery" for a hang,
because requests that hung and then failed over succeeded (no errors) and were
bucketed into the second they started. Recovery is now "first second with 0
errors AND p99 below half the provider timeout". Lesson: zero errors can hide a
2-second latency spike, so measure latency too, not just errors.

## D34. Sticky routing (session affinity) for multi-turn conversations

**Problem:** routing is per request. LLM APIs are stateless (each turn resends
the whole history), so switching providers between turns *works*, but it
throws away the provider's prompt cache (cheaper input tokens, faster TTFT on
the repeated prefix) and changes the model's style mid-conversation. With
`weighted`, a conversation switched providers randomly on every turn.
**Chose:** opt-in header `X-Gateway-Session: <conversation id>`. The gateway
remembers which deployment served the conversation's **last** turn and passes
it to the router as `Policy.Prefer`; `Plan` moves it to the front, after
strategy and exploration. Response header `X-Gateway-Sticky: new|hit|moved`.
**Design points:**
- *Last* turn, not first: after a failover, the prompt cache lives on the new
  provider, so the conversation stays there even when the old one recovers.
- Stickiness never overrides health: if the preferred deployment's breaker is
  open, it isn't in the plan at all, and normal failover applies.
- Store: Redis when configured (shared by all replicas), otherwise in memory
  (correct for one instance). Fails open with 5s backoff, like the cache.
- Key = SHA-256(API key id + session id + model group): one tenant can't steer
  another's conversations, and client ids of any shape become fixed-size keys.
- TTL 1h, refreshed every turn (`router.session_ttl`). Provider prompt caches
  expire after minutes idle, so remembering longer buys nothing.
**Alternatives:** consistent hashing of the session id over deployments (no
storage, but ignores strategy: with `priority` it would spread conversations
onto the backup); hashing the first message (no client change needed, but
collides for identical openings like "hi").
**Verified:** 30 conversations × 5 turns on a 90/10 weighted group: every
conversation used exactly one provider (27 on A, 3 on B); breaking the sticky
provider moved the conversation, and it stayed on the new one after healing.
