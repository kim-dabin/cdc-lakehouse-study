#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
# Stop scheduling first; do not discard the Flink recovery point.
./scripts/lab.sh --profile orchestration stop airflow
./scripts/lab.sh exec -T runner python scripts/flink_state.py stop
./scripts/lab.sh stop
