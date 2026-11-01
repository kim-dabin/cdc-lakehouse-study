#!/usr/bin/env bash
# Publish table metadata, dbt lineage (incl. column lineage) and dbt contracts to OpenMetadata.
# Needs the optional governance stack: ./scripts/lab.sh -f compose.yaml -f compose.governance.yaml up -d om-server
# Usage: ./scripts/governance.sh [--no-build]   (--no-build reuses the artifacts of the last dbt build)
set -euo pipefail
cd "$(dirname "$0")/.."
LAB=(./scripts/lab.sh -f compose.yaml -f compose.governance.yaml)
if [[ "${1:-}" != "--no-build" ]]; then
  # dbt build writes manifest.json (with compiled SQL for lineage) and run_results.json (test results).
  "${LAB[@]}" exec -T runner dbt build --project-dir dbt --profiles-dir dbt
fi
mkdir -p .runtime/om
token=$("${LAB[@]}" exec -T runner python governance/openmetadata/om_api.py token | tail -n 1)
for config in spark-metadata dbt; do
  sed "s|__OM_JWT__|${token}|" "governance/openmetadata/${config}.yaml" > ".runtime/om/${config}.yaml"
done
"${LAB[@]}" --profile governance-run run --rm -T om-ingestion ingest -c /workspace/.runtime/om/spark-metadata.yaml
"${LAB[@]}" --profile governance-run run --rm -T om-ingestion ingest -c /workspace/.runtime/om/dbt.yaml
"${LAB[@]}" exec -T runner python governance/openmetadata/om_api.py contracts
"${LAB[@]}" exec -T runner python governance/openmetadata/om_api.py lineage
echo "OpenMetadata: http://127.0.0.1:28585 (service cdc_lakehouse)"
