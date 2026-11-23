#!/usr/bin/env bash
# Cache benchmark: compares a guaranteed cache miss (X-Gateway-No-Cache: true)
# against a guaranteed cache hit (same request repeated with temperature 0)
# on the same gateway config. Needs Redis and config.bench.yaml (written by
# the other engineer, with cache enabled and redis.addr = localhost:6379).
#
#   ./bench/cache.sh
#   RPS=300 ./bench/cache.sh
set -euo pipefail
cd "$(dirname "$0")/.."

RPS=${RPS:-200}
DURATION=${DURATION:-20}  # seconds, per phase
CONFIG=${CONFIG:-config.bench.yaml}

if [ ! -f "$CONFIG" ]; then
  echo "missing $CONFIG (expected to be created alongside this script, with cache enabled and redis.addr set)." >&2
  exit 1
fi

if ! nc -z localhost 6379 2>/dev/null; then
  echo "redis not reachable on localhost:6379." >&2
  echo "  hint: docker run -d -p 6379:6379 redis:7-alpine" >&2
  exit 1
fi

make build >/dev/null
go build -o bin/loadgen ./bench/loadgen

TS=$(date +%Y%m%d-%H%M%S)
OUT="bench/results/cache-${TS}"
mkdir -p "$OUT"

PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

./bin/mockprovider -addr :9001 -name mock-a > "$OUT/mock-a.log" 2>&1 &
PIDS+=("$!")
./bin/gateway -config "$CONFIG" -log-level warn > "$OUT/gateway.log" 2>&1 &
PIDS+=("$!")
sleep 1

{
  echo "date: $(date)"
  echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
  echo "cpus: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
  echo "go: $(go version)"
  echo "config: $CONFIG"
  echo "rps: $RPS"
  echo "duration per phase: ${DURATION}s"
} > "$OUT/env.txt"

echo ">>> phase 1: miss path (X-Gateway-No-Cache: true, temperature 0)"
./bin/loadgen -mode rate -url http://localhost:8080/v1 -model mock-openai-style \
  -rps "$RPS" -duration "${DURATION}s" -temperature 0 \
  -header "X-Gateway-No-Cache:true" \
  -csv "$OUT/miss.csv" | tee "$OUT/miss.txt"

echo ">>> warming cache with one request"
curl -s -o /dev/null http://localhost:8080/v1/chat/completions \
  -d '{"model":"mock-openai-style","temperature":0,"messages":[{"role":"user","content":"Say hello in one sentence."}]}'
sleep 1

echo ">>> phase 2: hit path (same request repeated, temperature 0, cache enabled)"
./bin/loadgen -mode rate -url http://localhost:8080/v1 -model mock-openai-style \
  -rps "$RPS" -duration "${DURATION}s" -temperature 0 \
  -csv "$OUT/hit.csv" | tee "$OUT/hit.txt"

{
  echo "cache benchmark — rps=$RPS duration=${DURATION}s per phase"
  echo "measured on this machine only."
  echo
  echo "miss path (see $OUT/miss.txt for full latency/cache distribution)"
  grep -E 'latency ms|cache distribution' "$OUT/miss.txt" || true
  echo
  echo "hit path (see $OUT/hit.txt for full latency/cache distribution)"
  grep -E 'latency ms|cache distribution' "$OUT/hit.txt" || true
} | tee "$OUT/summary.txt"

echo
echo "raw results: $OUT"
