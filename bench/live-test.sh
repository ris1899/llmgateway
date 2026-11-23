#!/usr/bin/env bash
# Live smoke test against REAL providers (Groq + Gemini, free tier).
#
# Needs: .env.local (LLMGW_GROQ_KEY, LLMGW_GEMINI_KEY) and config.local.yaml.
# Run:   ./bench/live-test.sh
# Output is also saved to bench/results/live-<timestamp>.txt (no keys in it).
#
# About 20 requests, spaced out to stay under free-tier rate limits (~1.5 min).
set -u
cd "$(dirname "$0")/.."

[ -f .env.local ] || { echo "missing .env.local"; exit 1; }
[ -f config.local.yaml ] || { echo "missing config.local.yaml"; exit 1; }
# shellcheck disable=SC1091
source .env.local

mkdir -p bench/results
OUT="bench/results/live-$(date +%Y%m%d-%H%M%S).txt"
exec > >(tee "$OUT") 2>&1

GROQ_MODEL=openai/gpt-oss-20b
GEMINI_MODEL=gemini-flash-lite-latest

echo "== 0. Network: can we reach the providers? (no keys sent)"
blocked=0
for u in https://api.groq.com/openai/v1/models https://generativelanguage.googleapis.com/v1beta/openai/models; do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 "$u")
  echo "  $code  $u"
  # 401/403/404 = reached the provider (it just wants a key). 307 = a proxy is
  # intercepting HTTPS: stop before any key is sent through it.
  [ "$code" = "307" ] || [ "$code" = "000" ] && blocked=1
done
if [ "$blocked" = 1 ]; then
  echo "BLOCKED: 307 = a proxy is intercepting HTTPS, 000 = no connection. Stopping before sending keys."
  exit 1
fi

echo
echo "== 1. Keys + model names"
code=$(curl -s -o /tmp/live-groq-models.json -w '%{http_code}' https://api.groq.com/openai/v1/models -H "Authorization: Bearer $LLMGW_GROQ_KEY")
echo "  groq   key: HTTP $code (200 = key OK)"
jq -r '.data[]?.id' /tmp/live-groq-models.json 2>/dev/null | grep -qx "$GROQ_MODEL" \
  && echo "  groq   model $GROQ_MODEL: found" \
  || { echo "  groq   model $GROQ_MODEL: NOT FOUND. Available llama models:"; jq -r '.data[]?.id' /tmp/live-groq-models.json 2>/dev/null | grep -i llama | sed 's/^/    /'; }

code=$(curl -s -o /tmp/live-gem-models.json -w '%{http_code}' https://generativelanguage.googleapis.com/v1beta/openai/models -H "Authorization: Bearer $LLMGW_GEMINI_KEY")
echo "  gemini key: HTTP $code (200 = key OK)"
jq -r '.data[]?.id' /tmp/live-gem-models.json 2>/dev/null | sed 's#^models/##' | grep -qx "$GEMINI_MODEL" \
  && echo "  gemini model $GEMINI_MODEL: found" \
  || { echo "  gemini model $GEMINI_MODEL: NOT FOUND. Available flash models:"; jq -r '.data[]?.id' /tmp/live-gem-models.json 2>/dev/null | grep -i flash | sed 's/^/    /'; }

echo
echo "== 2. Start gateway"
if curl -s -o /dev/null localhost:8080/healthz; then
  echo "  port 8080 already in use: run 'make stop' / 'make down' first"; exit 1
fi
[ -x bin/gateway ] || go build -o bin/gateway ./cmd/gateway || exit 1
bin/gateway -config config.local.yaml > /tmp/live-gateway.log 2>&1 &
GW=$!
trap 'kill $GW 2>/dev/null' EXIT
for _ in $(seq 1 20); do curl -s -o /dev/null localhost:8080/readyz && break; sleep 0.5; done
curl -s -o /dev/null localhost:8080/readyz || { echo "  gateway did not start:"; tail -5 /tmp/live-gateway.log; exit 1; }
echo "  gateway up (log: /tmp/live-gateway.log)"

ask() { # ask <model group>: prints status, routing headers, first line of answer
  curl -s -D /tmp/live-h -o /tmp/live-b --max-time 60 localhost:8080/v1/chat/completions \
    -d "{\"model\":\"$1\",\"messages\":[{\"role\":\"user\",\"content\":\"Say hi in 5 words\"}]}"
  printf '  %-14s %s | ' "$1" "$(head -1 /tmp/live-h | tr -d '\r')"
  grep -iE '^x-gateway-(provider|attempts|upstream-ms):' /tmp/live-h | tr -d '\r' | sed 's/^x-gateway-//I' | tr '\n' ' '
  echo
  printf '      -> %s\n' "$(jq -r '.choices[0].message.content // .error.message // "?"' /tmp/live-b 2>/dev/null | head -1 | cut -c1-100)"
}

echo
echo "== 3. Each real provider, non-streaming"
ask live-groq
ask live-gemini

echo
echo "== 4. Streaming (Groq)"
curl -sN --max-time 60 localhost:8080/v1/chat/completions \
  -d '{"model":"live-groq","stream":true,"messages":[{"role":"user","content":"Count to 5"}]}' > /tmp/live-stream.txt
echo "  data lines: $(grep -c '^data:' /tmp/live-stream.txt)   ended with [DONE]: $(grep -q '^data: \[DONE\]' /tmp/live-stream.txt && echo yes || echo NO)"
head -3 /tmp/live-stream.txt | cut -c1-140 | sed 's/^/    /'

echo
echo "== 5. Real failover: primary (Gemini) has a 1ms timeout, Groq must answer"
echo "   expect: first 5 attempts=2 (timeout, then failover); then attempts=1 (breaker open, primary skipped)"
# No sleep on purpose: requests must land inside the breaker's 2s cooldown.
# Spaced >2s apart, every request after the 5th becomes a half-open probe
# (attempts=2 again), which is correct breaker behaviour but hides the open state.
for i in 1 2 3 4 5 6 7; do ask live-failover; done

echo
echo "== 6. Fastest strategy: which provider does it pick?"
for i in 1 2 3 4 5 6; do ask live-fast; sleep 3; done

echo
echo "== 7. Gateway's view (/admin/deployments)"
curl -s localhost:8081/admin/deployments | jq -c '.' 2>/dev/null | cut -c1-2000

echo
echo "Saved to $OUT"
