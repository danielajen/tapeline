#!/usr/bin/env bash
# Installs and starts a single-node Flink cluster configured for this repo.
#
# This lives in a script rather than inline in a workflow because every line
# below is a defect that was hit for real (POSTMORTEM 3), and one of them was
# already reintroduced once by a copy that drifted. Two workflows now need
# this setup; duplicating it would guarantee the same class of regression a
# third time.
#
#   FLINK_VERSION=1.20.1 scripts/install-flink.sh
#
# Exports nothing. Prints the Flink home directory on the last line so callers
# can capture it.
set -euo pipefail

FLINK_VERSION="${FLINK_VERSION:-1.20.1}"
SLOTS="${FLINK_SLOTS:-4}"
PARALLELISM="${FLINK_PARALLELISM:-2}"
CHECKPOINT_INTERVAL="${FLINK_CHECKPOINT_INTERVAL:-10s}"
TM_MEMORY="${FLINK_TM_MEMORY:-2400m}"

F="/tmp/flink-${FLINK_VERSION}"

if [ ! -d "$F" ]; then
  curl -fsSL -o /tmp/flink.tgz \
    "https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
  tar -xzf /tmp/flink.tgz -C /tmp
fi

# DEFECT 1: the job is Scala 2.13; the distribution bundles 2.12 and wins on
# the client classpath, producing NoSuchMethodError: scala.Predef$.refArrayOps.
# The job uses Flink's JAVA API (DESIGN_DECISIONS d3), so this jar is simply
# unnecessary.
mkdir -p "$F/opt-disabled"
mv "$F/lib/flink-scala_2.12-${FLINK_VERSION}.jar" "$F/opt-disabled/" 2>/dev/null || true

# DEFECT 2: the Kafka client must be parent-loaded, or a job restart leaves
# Kafka threads holding a closed classloader and the next attempt dies with
# ClassNotFoundException: NetworkClient$1.
find ~/.m2 -name 'flink-connector-kafka-*.jar' -exec cp {} "$F/lib/" \; 2>/dev/null || true
find ~/.m2 -name 'kafka-clients-*.jar' ! -name '*sources*' -exec cp {} "$F/lib/" \; 2>/dev/null || true

# DEFECT 4: config.yaml is OVERWRITTEN, not appended to.
#
# Flink 1.20 ships a config.yaml that already defines jobmanager, taskmanager,
# state, rest and parallelism as top-level keys. Appending our own copies
# produces duplicate top-level YAML keys, the file stops parsing, and the
# cluster refuses to start - with an error about YAML rather than about
# anything we changed. Writing a complete config from scratch is the only way
# to be sure of what the cluster actually reads.
cat > "$F/conf/config.yaml" <<CONF
jobmanager:
  rpc:
    address: localhost
    port: 6123
  bind-host: localhost
  memory:
    process:
      size: 1600m
taskmanager:
  bind-host: localhost
  host: localhost
  numberOfTaskSlots: ${SLOTS}
  memory:
    process:
      size: ${TM_MEMORY}
parallelism:
  default: ${PARALLELISM}
rest:
  address: localhost
  bind-address: localhost
  port: 8090
  bind-port: 8090
state:
  backend:
    type: hashmap
  checkpoints:
    dir: file:///tmp/flink-ckpt
execution:
  checkpointing:
    interval: ${CHECKPOINT_INTERVAL}
    min-pause: 2s
    timeout: 2min
classloader:
  parent-first-patterns:
    additional: org.apache.kafka
env:
  java:
    opts:
      # DEFECT 5: Flink 1.20 reflects into java.util during static
      # initialisation and dies with InaccessibleObjectException on any JDK
      # with the module system enforced. These flags were added when the
      # defect first appeared, then lost when DEFECT 4's fix replaced the
      # whole config file rather than appending to it - the same bug
      # reappeared one step later in the pipeline. They apply to the cluster
      # JVMs; the client JVM needs FLINK_ENV_JAVA_OPTS separately.
      all: "--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.time=ALL-UNNAMED"
CONF

mkdir -p /tmp/flink-ckpt
echo "--- effective config ---" >&2
cat "$F/conf/config.yaml" >&2

"$F/bin/start-cluster.sh" >&2

for _ in $(seq 1 30); do
  curl -fsS http://localhost:8090/overview >/dev/null 2>&1 && break
  sleep 2
done

echo "$F"
