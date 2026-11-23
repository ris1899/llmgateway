# Benchmarks

All benchmarks run everything (mocks, gateway, load generator) natively on one
machine. Honest caveats before reading any number here:

- **One laptop.** The load generator, the gateway, and the mock providers all
  compete for the same CPU cores. A busy generator can starve the gateway (or
  vice versa), which inflates or deflates latency in ways a real deployment
  (separate machines) would not show.
- **Mocks are not real providers.** `mockprovider` has configurable fixed
  latency/jitter/error rates, but no real network path, no real model
  inference variance, no real provider-side queuing. These numbers show the
  gateway's own overhead and failover behavior, not what OpenAI/Anthropic
  actually cost you in latency.
- **macOS fd limits.** Default `ulimit -n` is often 256-10240 depending on
  shell/session. High-concurrency streaming tests need it raised (scripts try
  to raise their own soft limit, but the hard limit and the kernel's
  `kern.ipc.somaxconn` need manual changes — see `bench/streaming.sh` output).
- **No numbers are invented here.** Every table below is empty until you run
  the corresponding script on your own machine.

## Tools

- `bench/loadgen/main.go` — single-file Go load generator (stdlib only).
  `go build -o bin/loadgen ./bench/loadgen`. Two modes:
  - `-mode rate`: open-model constant arrival rate (coordinated-omission
    free — each tick launches a request regardless of outstanding ones).
  - `-mode streams`: holds N concurrent SSE streams open for a ramp+hold
    window.
  See `-h` for all flags (`-rps`, `-duration`, `-conns`, `-ramp`, `-hold`,
  `-stream`, `-temperature`, `-header k:v`, `-csv`, `-timeline`, ...).
- `bench/k6/chat.js` + `bench/run-baseline.sh` — k6-based rate/latency sweep,
  direct-to-mock vs through-gateway (falls back to pointing you at loadgen if
  k6 isn't installed; the k6 path itself is unchanged).
- `bench/failover.sh` — breaker/failover timing.
- `bench/cache.sh` — cache hit vs miss latency.
- `bench/streaming.sh` — TTFT overhead and max concurrent streams.

## What each script measures and exact commands

### `bench/run-baseline.sh`
Throughput/latency at several fixed rates, direct-to-mock vs through-gateway,
using k6's constant-arrival-rate executor.
```
brew install k6
./bench/run-baseline.sh
RATES="500 1000 2000" ./bench/run-baseline.sh
```

### `bench/failover.sh`
Drives load at model `mock-chat` (mock-a priority 0, mock-b priority 1), kills
mock-a (or injects errors/hangs) at t=20s, restores it at t=40s, and measures
how fast the breaker reroutes traffic and how fast it notices mock-a is
healthy again.
```
./bench/failover.sh                     # MODE=kill, RPS=500, 60s
MODE=error ./bench/failover.sh
MODE=hang RPS=200 ./bench/failover.sh
```
Writes `bench/results/failover-<mode>-<timestamp>/`:
- `env.txt` — date, CPU (`sysctl machdep.cpu.brand_string`), core count, go
  version, mode, rps.
- `timeline.csv` — per-second sent/ok/errors/p50/p99/provider-distribution,
  produced by loadgen's `-timeline`.
- `breaker.log` — snapshot of `GET :8081/admin/deployments` every 500ms with a
  unix timestamp, so you can see breaker state transitions over time.
- `events.log` — when the fault was injected/restored.
- `summary.txt` — total errors, errors during the fault window, "recovery
  time" (seconds from fault injection until the first full second with zero
  requests to mock-a and zero errors), and time after restore until mock-a
  receives traffic again. **These numbers are specific to this run on this
  machine** and are printed as such.

### `bench/cache.sh`
Needs Redis on `localhost:6379` and `config.bench.yaml` (cache enabled,
written separately). Runs a guaranteed-miss phase (`X-Gateway-No-Cache: true`,
`temperature: 0`) then a guaranteed-hit phase (same request, cache enabled),
reporting p50/p99 for both and the `X-Gateway-Cache` distribution.
```
docker run -d -p 6379:6379 redis:7-alpine   # if you don't have redis running
./bench/cache.sh
RPS=300 ./bench/cache.sh
```

### `bench/streaming.sh`
Part 1: TTFT direct-to-mock vs through-gateway at a modest streaming rate
(mock paced at 50ms-to-first-token, 50 tok/s), reporting both TTFT
distributions, the delta, and the gateway's own `X-Gateway-Overhead-Ms`.
Part 2: holds up to `CONNS` (default 10000) concurrent streams open
(ramp 20s, hold 30s, ~10s-lived streams) and reports peak concurrency reached,
completions, and errors.
```
./bench/streaming.sh
RPS=100 CONNS=5000 ./bench/streaming.sh
```
Prints macOS fd-limit guidance (`sudo launchctl limit maxfiles 65536 200000`,
checking `kern.ipc.somaxconn`) without running `sudo` itself.

## Reading results

- Every script's `env.txt` records exactly what machine/config/parameters
  produced the numbers next to it — treat numbers without a matching `env.txt`
  as untrustworthy.
- `*.csv` files are plain per-request or per-second rows; load into a
  spreadsheet or `pandas`/`duckdb` for your own percentile cuts if the
  console summary isn't enough.
- Percentiles printed by loadgen are computed in-process from all captured
  per-request latencies for that run (not sampled/estimated).

## Results template (fill in by running the scripts above)

| Metric | Value |
|---|---|
| Throughput/latency by rate (p50/p99 @ RPS) | (run to fill) |
| Gateway overhead p50/p99 (ms) | (run to fill) |
| Cache hit vs miss latency (p50/p99) | (run to fill) |
| Failover recovery time & errors — MODE=kill | (run to fill) |
| Failover recovery time & errors — MODE=error | (run to fill) |
| Failover recovery time & errors — MODE=hang | (run to fill) |
| TTFT overhead (gateway - direct, p50/p99) | (run to fill) |
| Max concurrent streams sustained | (run to fill) |
