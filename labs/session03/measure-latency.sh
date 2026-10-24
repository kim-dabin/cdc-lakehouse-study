#!/usr/bin/env bash
# One explicit common-example refund in the existing runner; never bootstrap.
set -euo pipefail
cd "$(dirname "$0")/../.."
exec ./scripts/lab.sh exec -T runner python -m labs.session03.measure_latency "$@"
