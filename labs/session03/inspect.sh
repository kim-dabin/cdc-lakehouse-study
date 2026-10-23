#!/usr/bin/env bash
# Read-only observations; evidence is printed, not written into shared folders.
set -euo pipefail
cd "$(dirname "$0")/../.."
case_id="${1:?Usage: ./labs/session03/inspect.sh s01-example-run}"
if [[ $# -ne 1 || ! "$case_id" =~ ^s01-[a-z0-9-]{3,48}$ ]]; then
  echo 'Use exactly one s01- case ID with lowercase letters, digits or hyphens.' >&2
  exit 2
fi
exec ./scripts/lab.sh exec -T runner python -m labs.session03.inspect_case "$case_id"
