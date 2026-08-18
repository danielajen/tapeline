#!/usr/bin/env bash
#
# Chaos experiments against the local stack.
#
#   ./chaos/run-chaos.sh broker      # kill the Kafka broker mid-stream
#   ./chaos/run-chaos.sh taskmanager # kill a Flink TaskManager
#   ./chaos/run-chaos.sh redis       # kill Redis under the serving tier
#   ./chaos/run-chaos.sh network     # partition ingestion from Kafka
#   ./chaos/run-chaos.sh all
#
# Each experiment states a hypothesis BEFORE it runs and then measures
# whether it held. That ordering is the whole point: an experiment where the
# expected result is written afterwards is not an experiment, it is a
# description. Every number in docs/MEASUREMENTS.md marked "chaos" is
# produced here, and the incident in docs/POSTMORTEM.md came out of the
# broker experiment failing its hypothesis on the first run.

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-deploy/docker-compose.yml}"
COMPOSE="docker compose -f ${COMPOSE_FILE}"
PROM="${PROM:-http://localhost:9091}"
RESULTS="${RESULTS:-chaos/results}"

mkdir -p "${RESULTS}"

log()  { printf '\033[1;34m[chaos]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[chaos]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[chaos]\033[0m %s\n' "$*" >&2; }

# Queries Prometheus for an instant value. Returns "NaN" when the series does
# not exist, which is distinguishable from a real zero.
promq() {
  local query="$1"
  curl -sG --data-urlencode "query=${query}" "${PROM}/api/v1/query" \
    | python3 -c '
import json, sys
try:
    payload = json.load(sys.stdin)
    result = payload["data"]["result"]
    print(result[0]["value"][1] if result else "NaN")
except Exception:
    print("NaN")
'
}

# Waits until a Prometheus expression is true, printing how long it took.
# This is how recovery time is measured: not by watching a dashboard, but by
# polling the same metric an alert would fire on.
wait_until() {
  local query="$1" timeout="${2:-120}" started elapsed value
  started=$(date +%s)
  while :; do
    value=$(promq "${query}")
    elapsed=$(( $(date +%s) - started ))
    if [[ "${value}" != "NaN" ]] && (( $(echo "${value} > 0" | bc -l) )); then
      echo "${elapsed}"
      return 0
    fi
    if (( elapsed >= timeout )); then
      echo "-1"
      return 1
    fi
    sleep 1
  done
}

baseline_throughput() {
  promq 'sum(rate(tapeline_ingest_events_published_total[1m]))'
}

record() {
  local name="$1" hypothesis="$2" outcome="$3" detail="$4"
  {
    echo "## ${name}"
    echo
    echo "**Run:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo
    echo "**Hypothesis:** ${hypothesis}"
    echo
    echo "**Outcome:** ${outcome}"
    echo
    echo "${detail}"
    echo
  } >> "${RESULTS}/chaos-log.md"
}

# --- Experiment 1: kill the Kafka broker -------------------------------------
#
# Hypothesis: the producer blocks and retries rather than dropping events.
# When the broker returns, published throughput recovers to baseline and the
# total published count over the window equals the total received count —
# meaning nothing was lost, only delayed.
chaos_broker() {
  log "experiment: kafka broker failure"
  local hypothesis="Producer retries rather than dropping. Zero events lost; throughput recovers within 60s."

  local before after recovery errors_before errors_after
  before=$(baseline_throughput)
  errors_before=$(promq 'sum(tapeline_ingest_publish_errors_total)')
  log "baseline publish throughput: ${before} events/sec"

  log "killing the broker"
  ${COMPOSE} kill kafka
  sleep 20

  log "publish errors during the outage: $(promq 'sum(tapeline_ingest_publish_errors_total)')"
  log "restarting the broker"
  ${COMPOSE} start kafka

  recovery=$(wait_until 'sum(rate(tapeline_ingest_events_published_total[1m])) > 0' 180 || true)
  after=$(baseline_throughput)
  errors_after=$(promq 'sum(tapeline_ingest_publish_errors_total)')

  log "recovered in ${recovery}s; throughput now ${after} events/sec"

  record "Kafka broker failure" "${hypothesis}" \
    "recovery ${recovery}s, throughput ${before} -> ${after} events/sec" \
    "Publish errors went from ${errors_before} to ${errors_after}. Compare
\`tapeline_ingest_events_received_total\` against
\`tapeline_ingest_events_published_total\` over the window: an equal delta is
the zero-loss claim. Any shortfall is real data loss and must be explained
before the number goes anywhere near a resume."
}

# --- Experiment 2: kill a Flink TaskManager ----------------------------------
#
# Hypothesis: the job restarts from its last checkpoint, order book state is
# restored rather than rebuilt from nothing, and bars for the affected window
# are recomputed identically.
chaos_taskmanager() {
  log "experiment: flink taskmanager failure"
  local hypothesis="Job restores from the last checkpoint; order book state survives; no duplicate committed output."

  local victim recovery
  victim=$(${COMPOSE} ps -q taskmanager | head -1)
  if [[ -z "${victim}" ]]; then
    warn "no taskmanager running; skipping"
    return
  fi

  log "checkpoints completed before: $(promq 'sum(flink_jobmanager_job_numberOfCompletedCheckpoints)')"
  log "killing taskmanager ${victim}"
  docker kill "${victim}" >/dev/null

  recovery=$(wait_until 'sum(rate(flink_taskmanager_job_task_operator_numRecordsOutPerSecond[1m])) > 0' 180 || true)
  log "output resumed after ${recovery}s"

  record "Flink TaskManager failure" "${hypothesis}" \
    "output resumed after ${recovery}s" \
    "The claim to verify is not that the job restarted — Flink always
restarts. It is that quotes after the restart are continuous with quotes
before it, which means the order book came back from the checkpoint rather
than being rebuilt empty. Check \`tapeline_book_snapshots_applied\`: a spike
means the books were rebuilt, and the state restore did not work."
}

# --- Experiment 3: kill Redis ------------------------------------------------
#
# Hypothesis: the rate limiter fails open and the API keeps serving, degraded
# but available. This is deliberate behaviour, not an accident, and the
# experiment exists to prove the deliberate part.
chaos_redis() {
  log "experiment: redis failure"
  local hypothesis="Rate limiter fails open. API stays available; quota enforcement is suspended and alerts."

  log "killing redis"
  ${COMPOSE} kill redis
  sleep 10

  local status
  status=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:8080/actuator/health" || echo "000")
  log "serving health during the outage: HTTP ${status}"

  ${COMPOSE} start redis
  sleep 10

  record "Redis failure" "${hypothesis}" \
    "serving health returned HTTP ${status} while Redis was down" \
    "Failing open is the intended design: a rate limiter is a protection, not
a dependency, and taking the API down to enforce a quota inverts the
priority. Cached quotes are unavailable during the outage, so GetQuote
returns NOT_FOUND — that is expected and is why the alert fires on the Redis
error rate rather than on request failures."
}

# --- Experiment 4: partition ingestion from Kafka ----------------------------
#
# Hypothesis: a network partition looks different from a broker crash. The
# producer cannot fail fast because there is nothing to refuse it, so it
# blocks until its timeout — which is the case most likely to expose an
# unbounded buffer.
chaos_network() {
  log "experiment: network partition between ingestion and kafka"
  local hypothesis="Producer blocks on timeouts; the fan-in buffer applies backpressure rather than growing without bound."

  local container
  container=$(${COMPOSE} ps -q ingestd | head -1)
  if [[ -z "${container}" ]]; then
    warn "ingestd not running; skipping"
    return
  fi

  log "disconnecting ingestd from the network"
  local network
  network=$(docker inspect "${container}" -f '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}' | head -1)
  docker network disconnect "${network}" "${container}"

  sleep 30
  log "queue depth during the partition: $(promq 'tapeline_ingest_pipeline_queue_depth')"

  docker network connect "${network}" "${container}"
  local recovery
  recovery=$(wait_until 'sum(rate(tapeline_ingest_events_published_total[1m])) > 0' 180 || true)

  record "Network partition" "${hypothesis}" \
    "recovered ${recovery}s after reconnection" \
    "The interesting metric here is \`tapeline_ingest_pipeline_queue_depth\`.
It should plateau at the configured buffer size rather than climbing, because
a full buffer must block the venue readers. A queue that keeps growing means
backpressure is not reaching the sockets and the process will eventually be
killed for memory."
}

main() {
  local experiment="${1:-all}"

  if ! curl -sf "${PROM}/-/healthy" >/dev/null 2>&1; then
    fail "Prometheus is not reachable at ${PROM}. Start the stack first:"
    fail "  docker compose -f ${COMPOSE_FILE} up -d"
    exit 1
  fi

  log "letting the pipeline reach steady state before perturbing it"
  sleep 30

  case "${experiment}" in
    broker)      chaos_broker ;;
    taskmanager) chaos_taskmanager ;;
    redis)       chaos_redis ;;
    network)     chaos_network ;;
    all)
      chaos_broker
      sleep 60
      chaos_taskmanager
      sleep 60
      chaos_redis
      sleep 30
      chaos_network
      ;;
    *)
      fail "unknown experiment: ${experiment}"
      fail "one of: broker, taskmanager, redis, network, all"
      exit 2
      ;;
  esac

  log "results appended to ${RESULTS}/chaos-log.md"
}

main "$@"
