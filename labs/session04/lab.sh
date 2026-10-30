#!/usr/bin/env bash
# Student-local Docker environment. No SSH or instructor-specific paths.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec ./scripts/lab.sh exec -T runner python labs/session04/lab.py "$@"
