#!/usr/bin/env bash
# Read-only Session 02 inspections; explicit DML is delegated to Session 01.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec python3 labs/session02/lab.py "$@"
