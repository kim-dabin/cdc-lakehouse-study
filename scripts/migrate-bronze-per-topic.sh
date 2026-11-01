#!/usr/bin/env bash
# One-time move of the shared Bronze from lake.bronze.cdc_events to one table per topic.
# Safe to rerun after a failure or after success. lake.bronze.cdc_events is kept as is.
# Procedure, checks and rollback: docs/operations.md "Bronze 토픽별 테이블 전환".
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime artifacts/runs
migrate() { ./scripts/lab.sh exec -T runner python scripts/migrate_bronze_per_topic.py "$@"; }
started=$(date +%s)
migrate stop        # savepoint stop of the single-table job: group offsets match the last Iceberg commit
migrate backfill    # create tables, copy history only into empty tables, verify; refuses on mismatch
set +e; migrate needs-submit; rc=$?; set -e
case "$rc" in
 10)
  ./scripts/lab.sh exec -T runner python scripts/configure.py --render-only
  ./scripts/lab.sh exec -T runner python scripts/flink_state.py prepare --fresh-state
  ./scripts/lab.sh exec -T jobmanager /opt/flink/bin/sql-client.sh -f /workspace/.runtime/submit.sql > artifacts/runs/flink-submit-bronze-per-topic.txt
  ./scripts/lab.sh exec -T runner python scripts/flink_state.py check
  ;;
 0) ;;
 *) exit "$rc" ;;
esac
migrate summary     # per-topic job running with a completed checkpoint; prints timings and row counts
echo "Migration script finished in $(( $(date +%s) - started )) s"
