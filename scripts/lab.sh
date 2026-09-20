#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .runtime/host.env ]]; then source .runtime/host.env; fi
exec docker compose "$@"
