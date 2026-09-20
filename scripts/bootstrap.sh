#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p .runtime artifacts/runs
./scripts/lab.sh up -d --build
for table in orders payments refunds; do
 ./scripts/lab.sh exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic "cdc.public.$table" --partitions 1 --replication-factor 1
done
./scripts/lab.sh exec -T runner python scripts/configure.py
if curl -fsS http://127.0.0.1:28081/jobs/overview | python3 -c 'import json,sys; sys.exit(not any(j["name"]=="cdc-bronze" and j["state"]=="RUNNING" for j in json.load(sys.stdin)["jobs"]))'; then
 echo 'cdc-bronze is already running; no duplicate submission.'
else
 ./scripts/lab.sh exec -T runner python scripts/flink_state.py prepare
 ./scripts/lab.sh exec -T jobmanager /opt/flink/bin/sql-client.sh -f /workspace/.runtime/submit.sql > artifacts/runs/flink-submit.txt
 ./scripts/lab.sh exec -T runner python scripts/flink_state.py check
fi
./scripts/lab.sh exec -T runner python scripts/pipeline.py --wait
