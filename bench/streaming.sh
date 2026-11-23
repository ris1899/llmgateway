#!/usr/bin/env bash
# Streaming benchmark: (1) TTFT overhead added by the gateway vs talking to
# the mock directly, at a modest rate; (2) how many concurrent streaming
# connections the gateway can hold open.
#
#   ./bench/streaming.sh
#   RPS=100 CONNS=5000 ./bench/streaming.sh
set -euo pipefail
cd "$(dirname "$0")/.."

RPS=${RPS:-200}
DURATION=${DURATION:-30}       # seconds, TTFT phase
CONNS=${CONNS:-10000}
RAMP=${RAMP:-20s}
HOLD=${HOLD:-30s}

make build >/dev/null
go build -o bin/loadgen ./bench/loadgen

echo "warning (macOS): default per-process fd limits are low. If you see connection"
echo "errors at high -conns, raise limits yourself before rerunning, e.g.:"
echo "  sudo launchctl limit maxfiles 65536 200000"
echo "  sysctl kern.ipc.somaxconn   # check/raise the listen backlog"
echo "This script will try to raise its own soft ulimit -n but cannot exceed the hard limit."

ulimit -n 65536 2>/dev/null || echo "warning: could not raise ulimit -n to 65536"

TS=$(date +%Y%m%d-%H%M%S)
OUT="bench/results/streaming-${TS}"
mkdir -p "$OUT"

PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

# 50ms to first token, 50 tok/s: realistic-ish pacing for the TTFT comparison.
./bin/mockprovider -addr :9001 -name mock-a -latency 50ms -tokens-per-sec 50 > "$OUT/mock-a.log" 2>&1 &
PIDS+=("$!")
./bin/gateway -config config.example.yaml -log-level warn > "$OUT/gateway.log" 2>&1 &
PIDS+=("$!")
sleep 1

{
  echo "date: $(date)"
  echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
  echo "cpus: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
  echo "go: $(go version)"
  echo "rps: $RPS  duration: ${DURATION}s"
  echo "conns: $CONNS  ramp: $RAMP  hold: $HOLD"
  echo "ulimit -n: $(ulimit -n)"
} > "$OUT/env.txt"

echo ">>> TTFT direct to mock"
./bin/loadgen -mode rate -stream -url http://localhost:9001/v1 -model mock-small \
  -rps "$RPS" -duration "${DURATION}s" -csv "$OUT/ttft-direct.csv" | tee "$OUT/ttft-direct.txt"

echo ">>> TTFT through gateway"
./bin/loadgen -mode rate -stream -url http://localhost:8080/v1 -model mock-openai-style \
  -rps "$RPS" -duration "${DURATION}s" -csv "$OUT/ttft-gateway.csv" | tee "$OUT/ttft-gateway.txt"

{
  echo "streaming TTFT comparison — rps=$RPS duration=${DURATION}s"
  echo "measured on this machine only; mock pacing: latency=50ms tokens_per_sec=50"
  echo
  echo "direct to mock:"
  grep -E 'ttft ms|latency ms' "$OUT/ttft-direct.txt" || true
  echo
  echo "through gateway:"
  grep -E 'ttft ms|latency ms|overhead ms' "$OUT/ttft-gateway.txt" || true
  echo
  echo "(difference between the two ttft p50/p99 lines above is the gateway's added TTFT;"
  echo " compare it to the X-Gateway-Overhead-Ms reported directly in the gateway run.)"
} | tee "$OUT/ttft-summary.txt"

# Slow tokens so each stream lives ~10s (50 tokens @ 5 tok/s), to test sustained
# concurrency rather than throughput.
echo
echo ">>> reconfiguring mock-a for concurrency test (tokens_per_sec=5, output_tokens=50)"
curl -s -X PATCH http://localhost:9001/admin/behavior -d '{"tokens_per_sec":5,"output_tokens":50}' >/dev/null

echo ">>> concurrency test: ramping to $CONNS streams over $RAMP, holding $HOLD"
./bin/loadgen -mode streams -url http://localhost:8080/v1 -model mock-openai-style \
  -conns "$CONNS" -ramp "$RAMP" -hold "$HOLD" | tee "$OUT/concurrency.txt"

echo
echo "raw results: $OUT"
