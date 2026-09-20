#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/lab.sh exec -T runner python scripts/flink_state.py stop
./scripts/lab.sh exec -T runner python scripts/configure.py
./scripts/lab.sh exec -T runner python scripts/flink_state.py prepare
./scripts/lab.sh exec -T jobmanager /opt/flink/bin/sql-client.sh -f /workspace/.runtime/submit.sql > artifacts/runs/flink-submit.txt
./scripts/lab.sh exec -T runner python scripts/flink_state.py check
