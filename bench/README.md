# Benchmarks

Everything runs natively on one machine: mocks, gateway and load generator
share the same CPU, and mocks have fixed latency. The numbers show gateway
overhead and failover behavior, not real-provider performance.

## `failover.sh`

Drives load at `mock-chat` (mock-a first, mock-b as fallback), breaks mock-a
at t=20s, restores it at t=40s, and measures errors and recovery.

```
./bench/failover.sh                     # MODE=kill, RPS=500, 60s
MODE=error ./bench/failover.sh          # every request returns 503
MODE=hang RPS=200 ./bench/failover.sh   # never answers
```

Each run writes `bench/results/failover-<mode>-<timestamp>/`:

- `summary.txt`: errors, p99, degraded seconds, recovery time
- `timeline.csv`: per-second sent/ok/errors/p50/p99/provider mix
- `breaker.log`: `GET :8081/admin/deployments` every 500ms
- `events.log`: when the fault started and ended
- `env.txt`: machine and run parameters
- `requests.csv`: one row per request

## `live-test.sh`

About 20 requests against real Groq and Gemini free tiers: each provider,
streaming, failover, and the `fastest` strategy. Needs keys in `.env.local`
(gitignored) and `config.local.yaml`. Stops before sending keys if an HTTPS
proxy is intercepting.

## `demo.sh`

Two mock providers, a streamed reply, and a live failover, for a quick look
(`make demo`).

`bench/loadgen` is the load generator `failover.sh` uses: fixed arrival
rate, so a slow server can't hide behind fewer requests. See `-h` for flags.
