#!/usr/bin/env bash
# Failover benchmark: drive load at model "mock-chat" (mock-a priority 0,
# mock-b priority 1) and take mock-a down mid-run, then bring it back.
# Measures how fast the gateway's breaker routes around the failure and how
# fast it notices mock-a is healthy again.
#
#   ./bench/failover.sh                 # MODE=kill, RPS=500, 60s run
#   MODE=error ./bench/failover.sh
#   MODE=hang RPS=200 ./bench/failover.sh
set -euo pipefail
cd "$(dirname "$0")/.."

MODE=${MODE:-kill}            # kill | error | hang
RPS=${RPS:-500}
DURATION=${DURATION:-60}      # seconds
FAULT_AT=${FAULT_AT:-20}      # seconds into the run
RESTORE_AT=${RESTORE_AT:-40}  # seconds into the run

case "$MODE" in
  kill|error|hang) ;;
  *) echo "MODE must be kill|error|hang, got $MODE" >&2; exit 1 ;;
esac

TS=$(date +%Y%m%d-%H%M%S)
OUT="bench/results/failover-${MODE}-${TS}"
mkdir -p "$OUT"

command -v go >/dev/null || { echo "go not found"; exit 1; }

make build >/dev/null
go build -o bin/loadgen ./bench/loadgen

ulimit -n 65536 2>/dev/null || echo "warning: could not raise ulimit -n"

PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

./bin/mockprovider -addr :9001 -name mock-a > "$OUT/mock-a.log" 2>&1 &
MOCK_A=$!
PIDS+=("$MOCK_A")
./bin/mockprovider -addr :9002 -name mock-b > "$OUT/mock-b.log" 2>&1 &
PIDS+=("$!")
./bin/gateway -config config.example.yaml -log-level warn > "$OUT/gateway.log" 2>&1 &
PIDS+=("$!")
sleep 1

{
  echo "date: $(date)"
  echo "cpu: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)"
  echo "cpus: $(sysctl -n hw.ncpu 2>/dev/null || nproc)"
  echo "go: $(go version)"
  echo "mode: $MODE"
  echo "rps: $RPS"
  echo "duration: ${DURATION}s"
  echo "fault_at: ${FAULT_AT}s restore_at: ${RESTORE_AT}s"
} > "$OUT/env.txt"

# Snapshot :8081/admin/deployments every 500ms with a timestamp, for the whole run.
(
  END=$(( $(date +%s) + DURATION + 3 ))
  while [ "$(date +%s)" -lt "$END" ]; do
    printf '%s ' "$(date +%s.%N)" >> "$OUT/breaker.log"
    curl -s http://localhost:8081/admin/deployments >> "$OUT/breaker.log" 2>/dev/null || echo '[]' >> "$OUT/breaker.log"
    echo >> "$OUT/breaker.log"
    sleep 0.5
  done
) &
PIDS+=("$!")

# Fault injection / restore, timed against the loadgen run in the background below.
(
  sleep "$FAULT_AT"
  echo "[$(date +%s)] injecting fault mode=$MODE" >> "$OUT/events.log"
  case "$MODE" in
    kill)
      kill "$MOCK_A" 2>/dev/null || true
      ;;
    error)
      curl -s -X PATCH http://localhost:9001/admin/behavior -d '{"error_rate":1,"error_status":503}' >/dev/null || true
      ;;
    hang)
      curl -s -X PATCH http://localhost:9001/admin/behavior -d '{"fault":"hang"}' >/dev/null || true
      ;;
  esac

  sleep "$(( RESTORE_AT - FAULT_AT ))"
  echo "[$(date +%s)] restoring mock-a" >> "$OUT/events.log"
  case "$MODE" in
    kill)
      ./bin/mockprovider -addr :9001 -name mock-a > "$OUT/mock-a-restarted.log" 2>&1 &
      ;;
    error)
      curl -s -X PATCH http://localhost:9001/admin/behavior -d '{"error_rate":0}' >/dev/null || true
      ;;
    hang)
      curl -s -X PATCH http://localhost:9001/admin/behavior -d '{"fault":""}' >/dev/null || true
      ;;
  esac
) &
PIDS+=("$!")

echo ">>> running loadgen at ${RPS} rps for ${DURATION}s against mock-chat"
./bin/loadgen -mode rate -url http://localhost:8080/v1 -model mock-chat \
  -rps "$RPS" -duration "${DURATION}s" \
  -timeline "$OUT/timeline.csv" -csv "$OUT/requests.csv" | tee "$OUT/loadgen.txt"

sleep 2 # let the background snapshot/event jobs finish their last tick

# --- post-processing ---
# total errors and errors during the fault window, straight from the timeline.
TOTAL_ERRORS=$(awk -F, 'NR>1{s+=$4} END{print s+0}' "$OUT/timeline.csv")
FAULT_WINDOW_ERRORS=$(awk -F, -v a="$FAULT_AT" -v b="$RESTORE_AT" 'NR>1 && $1>=a && $1<b {s+=$4} END{print s+0}' "$OUT/timeline.csv")

# Recovery time: seconds from fault injection until the first full second with
# zero errors AND p99 below SLOW_MS. Timeline buckets are by request *start*
# second, so requests that hung on mock-a and then failed over show up as a
# high p99 in the second they started, not as errors. Checking only errors
# would report 0s recovery for a hang, which is wrong. SLOW_MS defaults to
# half the provider timeout (2s in config.example.yaml): any second whose p99
# is below that had no meaningful share of requests waiting on a timeout.
SLOW_MS=${SLOW_MS:-1000}
RECOVERY_SEC=$(awk -F, -v a="$FAULT_AT" -v b="$RESTORE_AT" -v slow="$SLOW_MS" '
  NR>1 && $1>=a && $1<b {
    if ($4==0 && $6+0 < slow && !found) { print $1 - a; found=1 }
  }
  END { if (!found) print "not-reached" }
' "$OUT/timeline.csv")
# How many seconds of the fault window had a degraded p99 (>= SLOW_MS).
DEGRADED_SEC=$(awk -F, -v a="$FAULT_AT" -v b="$RESTORE_AT" -v slow="$SLOW_MS" \
  'NR>1 && $1>=a && $1<b && $6+0 >= slow {n++} END{print n+0}' "$OUT/timeline.csv")

# Traffic-resumed time: seconds after restore until mock-a receives traffic again.
RESUME_SEC=$(awk -F, -v b="$RESTORE_AT" '
  NR>1 && $1>=b {
    if ($7 ~ /mock-a/ && !found) { print $1 - b; found=1 }
  }
  END { if (!found) print "not-reached" }
' "$OUT/timeline.csv")

{
  echo "failover benchmark — MODE=$MODE RPS=$RPS duration=${DURATION}s"
  echo "measured on this machine only; not representative of production hardware or network."
  echo
  echo "total errors over run:            $TOTAL_ERRORS"
  echo "errors during fault window [${FAULT_AT}s,${RESTORE_AT}s): $FAULT_WINDOW_ERRORS"
  echo "recovery time after fault (s, first second with 0 errors and p99 < ${SLOW_MS}ms): $RECOVERY_SEC"
  echo "seconds in fault window with p99 >= ${SLOW_MS}ms:                $DEGRADED_SEC"
  echo "time after restore until mock-a receives traffic again (s):        $RESUME_SEC"
} | tee "$OUT/summary.txt"

echo
echo "raw results: $OUT"
