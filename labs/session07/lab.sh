#!/usr/bin/env bash
# 개인 Docker 환경에서 실행해요.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec ./scripts/lab.sh exec -T runner python labs/session07/lab.py "$@"
