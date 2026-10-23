#!/usr/bin/env bash
# Run from any directory; Python on the host needs only the standard library.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 scripts/self_study.py "$@"
