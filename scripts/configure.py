"""Render local-only config; never put actual credentials in tracked SQL."""
import json
import os
import time
from pathlib import Path
import requests

def wait(url):
    for _ in range(90):
        try:
            r = requests.get(url, timeout=3)
            if r.ok:
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError(f"Service did not become ready: {url}")

wait("http://connect:8083/connectors")
wait("http://jobmanager:8081/overview")
config = {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "source-db", "database.port": "5432",
    "database.user": "source", "database.password": os.environ["SOURCE_PASSWORD"],
    "database.dbname": "app", "topic.prefix": "cdc",
    "plugin.name": "pgoutput", "slot.name": "cdc_lab",
    "publication.name": "cdc_lab", "publication.autocreate.mode": "disabled",
    "table.include.list": "public.orders,public.payments,public.refunds",
    "snapshot.mode": "initial", "tombstones.on.delete": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false", "value.converter.schemas.enable": "false",
    "time.precision.mode": "connect", "heartbeat.interval.ms": "10000"
}
r = requests.put("http://connect:8083/connectors/source-postgres/config", json=config, timeout=20)
r.raise_for_status()
# SQL escaping applies to SQL string literals, not shell commands.
password = os.environ["CATALOG_PASSWORD"].replace("'", "''")
sql = Path("flink/ingest.sql.template").read_text().replace("__CATALOG_PASSWORD__", password)
sql = sql.replace("__CHECKPOINT_INTERVAL__", os.environ.get("CHECKPOINT_INTERVAL", "10s"))
Path(".runtime").mkdir(exist_ok=True)
Path(".runtime/ingest.sql").write_text(sql)
print("Connector registered; local Flink SQL rendered.")
