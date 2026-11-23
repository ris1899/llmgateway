#!/usr/bin/env bash
# Baseline benchmark: gateway vs direct-to-mock at several request rates.
# Runs everything natively on the host (not in Colima) so VM port forwarding
# doesn't distort the latency numbers. Results land in bench/results/.
#
#   brew install k6
#   ./bench/run-baseline.sh            # default rates
#   RATES="500 1000 2000" ./bench/run-baseline.sh
set -euo pipefail
cd "$(dirname "$0")/.."

RATES=${RATES:-"500 1000 2000 4000"}
DURATION=${DURATION:-30s}
OUT=bench/results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$OUT"

if ! command -v k6 >/dev/null; then
  echo "k6 not found: brew install k6"
  echo "falling back to the Go loadgen (bench/loadgen) for a rough rate/latency check instead:"
  echo "  go build -o bin/loadgen ./bench/loadgen && ./bin/loadgen -mode rate -rps 1000 -duration 30s -url http://localhost:8080/v1 -model mock-openai-style"
  echo "(this does not produce the k6-based direct-vs-gateway table below; install k6 for that)"
  exit 1
fi
ulimit -n 65536 2>/dev/null || echo "warning: could not raise ulimit -n"

make build >/dev/null
# Mock with fixed 50ms latency and no jitter: provider time is constant, so any
# difference between direct and via-gateway is the gateway.
./bin/mockprovider -addr :9001 -name mock-a -latency 50ms > "$OUT/mock.log" 2>&1 &
MOCK=$!
./bin/gateway -config config.example.yaml -log-level warn > "$OUT/gateway.log" 2>&1 &
GW=$!
trap 'kill $MOCK $GW 2>/dev/null' EXIT
sleep 1

{
  echo "date: $(date)"
  echo "host: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null) cpus"
  echo "go: $(go version)"
  echo "mock latency: 50ms fixed"
} > "$OUT/env.txt"

for rate in $RATES; do
  echo ">>> rate=$rate direct"
  k6 run -q -e RATE="$rate" -e DURATION="$DURATION" -e TARGET=http://localhost:9001/v1 -e MODEL=mock-small \
    --summary-export "$OUT/direct-$rate.json" bench/k6/chat.js > "$OUT/direct-$rate.txt" 2>&1 || true
  echo ">>> rate=$rate gateway"
  k6 run -q -e RATE="$rate" -e DURATION="$DURATION" \
    --summary-export "$OUT/gateway-$rate.json" bench/k6/chat.js > "$OUT/gateway-$rate.txt" 2>&1 || true
done

echo
echo "rate  | direct p50/p99 (ms) | gateway p50/p99 (ms) | overhead p50/p99 (ms) | gw rps | gw fail%"
for rate in $RATES; do
  d=$OUT/direct-$rate.json; g=$OUT/gateway-$rate.json
  jq -r --arg r "$rate" --slurpfile d "$d" '
    [$r,
     ($d[0].metrics.http_req_duration["p(50)"]|.*100|round/100), ($d[0].metrics.http_req_duration["p(99)"]|.*100|round/100),
     (.metrics.http_req_duration["p(50)"]|.*100|round/100), (.metrics.http_req_duration["p(99)"]|.*100|round/100),
     (.metrics.gateway_overhead_ms["p(50)"]|.*1000|round/1000), (.metrics.gateway_overhead_ms["p(99)"]|.*1000|round/1000),
     (.metrics.http_reqs.rate|round), (.metrics.http_req_failed.value*100|.*100|round/100)]
    | "\(.[0]) | \(.[1]) / \(.[2]) | \(.[3]) / \(.[4]) | \(.[5]) / \(.[6]) | \(.[7]) | \(.[8])"' "$g"
done | tee "$OUT/summary.txt"
echo
echo "raw results: $OUT"
