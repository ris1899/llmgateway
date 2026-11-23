#!/usr/bin/env bash
# Week 1 demo: one API, different providers, live failover.
# Needs the gateway on :8080 and mocks on :9001-9003 (make mocks && make run,
# or make up).
set -euo pipefail
GW=${GW:-http://localhost:8080}

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }
chat() {
  curl -s -D - -o /tmp/demo-body "$GW/v1/chat/completions" \
    -d "{\"model\":\"$1\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}" \
    | grep -iE '^x-gateway-(provider|attempts|overhead-ms|cost-usd)'
  jq -r '.choices[0].message.content' /tmp/demo-body
}

say "1) Same API, OpenAI-format provider"
chat mock-openai-style

say "2) Same API, Anthropic-format provider (system prompt, max_tokens, events all translated)"
chat mock-claude-style

say "3) Streaming, tokens arrive one by one"
curl -sN "$GW/v1/chat/completions" \
  -d '{"model":"mock-chat","stream":true,"messages":[{"role":"user","content":"hi"}]}' \
  | while IFS= read -r line; do
      if [[ $line == data:* && $line != *DONE* ]]; then
        jq -rj '.choices[0].delta.content // empty' <<<"${line#data: }"
      fi
    done
echo

say "4) Break mock-a (every request -> 503) and call the model group again"
curl -s -X PATCH localhost:9001/admin/behavior -d '{"error_rate":1,"error_status":503}' >/dev/null
chat mock-chat
curl -s -X PATCH localhost:9001/admin/behavior -d '{"error_rate":0}' >/dev/null
say "mock-a restored"
