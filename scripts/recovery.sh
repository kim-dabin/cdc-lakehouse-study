#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/lab.sh exec -T runner python scripts/recovery.py prepare
trap './scripts/lab.sh start taskmanager >/dev/null' EXIT
./scripts/lab.sh stop taskmanager
./scripts/lab.sh exec -T runner python scripts/recovery.py mutate
./scripts/lab.sh start taskmanager
./scripts/lab.sh exec -T runner python scripts/recovery.py check
