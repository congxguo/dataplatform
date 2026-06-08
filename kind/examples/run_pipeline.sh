#!/usr/bin/env bash
# run_pipeline.sh — Bootstraps and runs the full ads pipeline on a kind cluster.
#
# Usage:
#   ./run_pipeline.sh setup          # one-time: create Kafka topic + Scylla schema
#   ./run_pipeline.sh produce        # run producer forever (Ctrl-C to stop)
#   ./run_pipeline.sh produce-1h     # run producer for exactly 1 hour
#   ./run_pipeline.sh flink          # submit the Flink job
#   ./run_pipeline.sh all            # setup + flink job + 1-hour producer run
#
# Prerequisites: kubectl, kafka-topics.sh (or kafkacat), cqlsh, pip install kafka-python

set -euo pipefail

# ── Cluster-local service addresses (via port-forward or NodePort) ─────────────
KAFKA_HOST="localhost:9092"
SCYLLA_HOST="localhost:9042"
FLINK_REST="http://localhost:8081"
KAFKA_POD_LABEL="app=kafka"          # adjust to your helm chart label
FLINK_JM_LABEL="component=jobmanager"
FLINK_NAMESPACE="flink"
KAFKA_NAMESPACE="kafka"
SCYLLA_NAMESPACE="scylla"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Helpers ───────────────────────────────────────────────────────────────────

log()  { echo -e "\033[1;36m[pipeline]\033[0m $*"; }
ok()   { echo -e "\033[1;32m[ok]\033[0m $*"; }
err()  { echo -e "\033[1;31m[error]\033[0m $*" >&2; exit 1; }

port_forward() {
    local ns=$1 label=$2 local_port=$3 remote_port=$4
    log "Port-forwarding $label → localhost:$local_port"
    kubectl port-forward -n "$ns" "$(kubectl get pod -n "$ns" -l "$label" \
        -o jsonpath='{.items[0].metadata.name}')" \
        "${local_port}:${remote_port}" &>/tmp/pf_${local_port}.log &
    PF_PIDS+=($!)
    sleep 2
}

cleanup() {
    log "Cleaning up port-forwards..."
    for pid in "${PF_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

PF_PIDS=()

# ── Setup: Kafka topic + ScyllaDB schema ──────────────────────────────────────

cmd_setup() {
    log "=== Setup ==="

    # Port-forward Kafka
    port_forward "$KAFKA_NAMESPACE" "app=kafka" 9092 9092

    # Create topic (idempotent)
    log "Creating Kafka topic 'ads-events'..."
    kubectl exec -n "$KAFKA_NAMESPACE" \
        "$(kubectl get pod -n "$KAFKA_NAMESPACE" -l "$KAFKA_POD_LABEL" \
            -o jsonpath='{.items[0].metadata.name}')" \
        -- kafka-topics.sh \
            --bootstrap-server localhost:9092 \
            --create \
            --if-not-exists \
            --topic ads-events \
            --partitions 6 \
            --replication-factor 1 \
            --config retention.ms=3600000   # keep 1 h of data
    ok "Kafka topic ready"

    # Port-forward ScyllaDB
    port_forward "$SCYLLA_NAMESPACE" "app=scylla" 9042 9042

    # Apply CQL schema
    log "Applying ScyllaDB schema..."
    cqlsh "$SCYLLA_HOST" -f "$SCRIPT_DIR/scylla_schema.cql"
    ok "ScyllaDB schema applied"
}

# ── Flink job submission ───────────────────────────────────────────────────────

cmd_flink() {
    log "=== Submitting Flink job ==="
    port_forward "$FLINK_NAMESPACE" "$FLINK_JM_LABEL" 8081 8081

    # Copy job file into the JobManager pod
    local jm_pod
    jm_pod="$(kubectl get pod -n "$FLINK_NAMESPACE" -l "$FLINK_JM_LABEL" \
        -o jsonpath='{.items[0].metadata.name}')"

    log "Copying job to pod $jm_pod..."
    kubectl cp "$SCRIPT_DIR/ads_flink_job.py" \
        "$FLINK_NAMESPACE/$jm_pod:/opt/flink/jobs/ads_flink_job.py"

    log "Submitting via REST API..."
    # Using Flink REST API (alternatively: flink run -py ...)
    curl -s -X POST "$FLINK_REST/jars/upload" || true

    kubectl exec -n "$FLINK_NAMESPACE" "$jm_pod" -- \
        /opt/flink/bin/flink run \
            --python /opt/flink/jobs/ads_flink_job.py \
            --pyExecutable python3

    ok "Flink job submitted — check $FLINK_REST for status"
}

# ── Producer runs ─────────────────────────────────────────────────────────────

cmd_produce() {
    log "=== Starting producer (forever, Ctrl-C to stop) ==="
    port_forward "$KAFKA_NAMESPACE" "$KAFKA_POD_LABEL" 9092 9092
    pip install kafka-python -q
    python3 "$SCRIPT_DIR/ads_producer.py" --rate 10
}

cmd_produce_1h() {
    log "=== Starting producer (1-hour run) ==="
    port_forward "$KAFKA_NAMESPACE" "$KAFKA_POD_LABEL" 9092 9092
    pip install kafka-python -q
    python3 "$SCRIPT_DIR/ads_producer.py" --rate 10 --duration 3600
    ok "1-hour producer run complete"
}

# ── All-in-one ────────────────────────────────────────────────────────────────

cmd_all() {
    cmd_setup
    cmd_flink
    cmd_produce_1h
}

# ── Dispatch ──────────────────────────────────────────────────────────────────

CMD="${1:-help}"
case "$CMD" in
    setup)       cmd_setup ;;
    flink)       cmd_flink ;;
    produce)     cmd_produce ;;
    produce-1h)  cmd_produce_1h ;;
    all)         cmd_all ;;
    *)
        echo "Usage: $0 {setup|produce|produce-1h|flink|all}"
        exit 1
        ;;
esac
