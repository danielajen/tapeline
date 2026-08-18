#!/usr/bin/env bash
#
# End-to-end smoke test against the local stack.
#
#   ./scripts/smoke-test.sh
#
# Walks one event from a venue socket to a served quote and checks each hop.
# The value is in the ordering: when it fails, the first failing step names
# the tier to look at, instead of leaving "the API returns nothing" to be
# debugged from the outside in.

set -euo pipefail

HTTP="${TAPELINE_HTTP:-http://localhost:8080}"
REGISTRY="${TAPELINE_SCHEMA_REGISTRY_URL:-http://localhost:8081}"
METRICS="${TAPELINE_INGEST_METRICS:-http://localhost:9101}"
KEY_ID="${TAPELINE_KEY_ID:-tk_local_dev}"
SECRET="${TAPELINE_SECRET:-local-development-secret-not-for-production}"
SYMBOL="${SYMBOL:-BTC-USD}"

pass() { printf '  \033[0;32mok\033[0m    %s\n' "$*"; }
fail() { printf '  \033[0;31mFAIL\033[0m  %s\n' "$*" >&2; exit 1; }
info() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }

info "1. ingestion is decoding venue frames"
received=$(curl -sf "${METRICS}/metrics" \
  | awk '/^tapeline_ingest_events_received_total/ {sum += $2} END {print sum + 0}')
[[ "${received:-0}" -gt 0 ]] \
  && pass "ingested ${received} events" \
  || fail "no events ingested — check ingestd logs and venue connectivity"

info "2. schemas are registered"
subjects=$(curl -sf "${REGISTRY}/subjects")
for subject in md.trades.v1-value md.book.v1-value; do
  echo "${subjects}" | grep -q "${subject}" \
    && pass "${subject} registered" \
    || fail "${subject} missing — ingestd could not register its schemas"
done

info "3. the registry is enforcing FULL compatibility"
level=$(curl -sf "${REGISTRY}/config" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("compatibilityLevel","?"))')
[[ "${level}" == "FULL" ]] \
  && pass "compatibility level is FULL" \
  || fail "compatibility level is ${level}; a rolling deploy is not safe below FULL"

info "4. events are reaching Kafka"
published=$(curl -sf "${METRICS}/metrics" \
  | awk '/^tapeline_ingest_events_published_total/ {sum += $2} END {print sum + 0}')
[[ "${published:-0}" -gt 0 ]] \
  && pass "published ${published} events" \
  || fail "nothing published — check broker health and topic existence"

info "5. the serving tier answers an authenticated request"
path="/api/v1/quotes/${SYMBOL}"
timestamp=$(date +%s)
nonce="smoke-$(date +%s%N)"
body_hash=$(printf '' | openssl dgst -sha256 -hex | awk '{print $NF}')
canonical=$(printf 'GET\n%s\n%s\n%s\n%s' "${path}" "${timestamp}" "${nonce}" "${body_hash}")
signature=$(printf '%s' "${canonical}" \
  | openssl dgst -sha256 -hmac "${SECRET}" -binary | base64)

response=$(curl -sf \
  -H "X-Tapeline-Key: ${KEY_ID}" \
  -H "X-Tapeline-Signature: ${signature}" \
  -H "X-Tapeline-Timestamp: ${timestamp}" \
  -H "X-Tapeline-Nonce: ${nonce}" \
  "${HTTP}${path}") || fail "the quote request failed — check serving logs for the auth failure reason"

echo "${response}" | grep -q '"mid"' \
  && pass "served a quote: $(echo "${response}" | head -c 120)..." \
  || fail "response had no mid price: ${response}"

info "6. an unsigned request is rejected"
status=$(curl -s -o /dev/null -w '%{http_code}' "${HTTP}${path}")
[[ "${status}" == "401" || "${status}" == "403" ]] \
  && pass "unsigned request rejected with ${status}" \
  || fail "unsigned request returned ${status}; authentication is not being enforced"

info "7. a replayed nonce is rejected"
status=$(curl -s -o /dev/null -w '%{http_code}' \
  -H "X-Tapeline-Key: ${KEY_ID}" \
  -H "X-Tapeline-Signature: ${signature}" \
  -H "X-Tapeline-Timestamp: ${timestamp}" \
  -H "X-Tapeline-Nonce: ${nonce}" \
  "${HTTP}${path}")
[[ "${status}" == "401" || "${status}" == "403" ]] \
  && pass "replayed nonce rejected with ${status}" \
  || fail "the same nonce was accepted twice; replay protection is not working"

printf '\n\033[0;32mAll smoke checks passed.\033[0m\n'
