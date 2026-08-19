#!/usr/bin/env bash
# Runs the ingestion tier against live venues.
#
# Binance and Coinbase need no credentials - their public market data feeds
# are open. Only the Ethereum feed needs a key, and only because an archive
# node is expensive to run; the venue feeds are free.
#
#   scripts/run-live.sh
#
# Reads .env.local if present. That file is gitignored: the Alchemy WebSocket
# URL contains the API key in its path, so the whole URL is a secret and must
# not reach a public repository.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f .env.local ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env.local
  set +a
  echo "loaded .env.local"
else
  echo "no .env.local; running venues only, no on-chain feed"
fi

if [ "${TAPELINE_ONCHAIN_ENABLED:-false}" = "true" ]; then
  case "${TAPELINE_ONCHAIN_WS_URL:-}" in
    ""|*YOUR_KEY_HERE*)
      echo "TAPELINE_ONCHAIN_WS_URL is unset or still the placeholder." >&2
      echo "Fill it in .env.local, or set TAPELINE_ONCHAIN_ENABLED=false." >&2
      exit 1
      ;;
    wss://*) ;;
    *)
      echo "TAPELINE_ONCHAIN_WS_URL must be a wss:// URL." >&2
      exit 1
      ;;
  esac
  # Print the host only. Echoing the full URL would put the API key in a log,
  # a terminal buffer and a screen share.
  echo "on-chain: ${TAPELINE_ONCHAIN_CHAIN:-ethereum} via $(echo "$TAPELINE_ONCHAIN_WS_URL" | cut -d/ -f3)"
fi

echo "starting Kafka and Schema Registry..."
docker compose -f deploy/docker-compose.yml up -d kafka schema-registry
for _ in $(seq 1 60); do
  docker exec tapeline-kafka-1 kafka-broker-api-versions \
    --bootstrap-server kafka:29092 >/dev/null 2>&1 && break
  sleep 5
done
curl -fsS --retry 30 --retry-delay 5 --retry-all-errors \
  http://localhost:8081/subjects >/dev/null
echo "infrastructure up"

for spec in md.trades.v1:6 md.book.v1:6 md.quotes.v1:4 md.chain.v1:4; do
  docker exec tapeline-kafka-1 kafka-topics --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "${spec%%:*}" \
    --partitions "${spec##*:}" --replication-factor 1 >/dev/null
done

cd ingest && go build -o /tmp/ingestd ./cmd/ingestd && cd ..
echo "running. ctrl-c to stop."
exec /tmp/ingestd
