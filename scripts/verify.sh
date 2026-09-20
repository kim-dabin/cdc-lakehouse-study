#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./scripts/lab.sh exec -T runner python scripts/verify.py
