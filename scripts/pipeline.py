"""Spark SQL Silver refresh, then dbt Gold models/tests. No shell interpolation."""
import fcntl
import argparse
import json
import subprocess
import time
from pathlib import Path
from pyhive import hive

MODELS = {
    "orders": ("order_id", {"order_id":"STRING", "ordered_at":"TIMESTAMP", "status":"STRING"}),
    "payments": ("payment_id", {"payment_id":"STRING", "order_id":"STRING", "amount":"BIGINT", "status":"STRING"}),
    "refunds": ("refund_id", {"refund_id":"STRING", "payment_id":"STRING", "amount":"BIGINT", "status":"STRING"}),
}

def connect(wait=False):
    for attempt in range(90 if wait else 1):
        try:
            return hive.Connection(host="spark", port=10000, username="lab", auth="NOSASL")
        except Exception:
            if not wait or attempt == 89:
                raise
            time.sleep(2)

def query(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()

def refresh(cursor):
    for table, (pk, fields) in MODELS.items():
        ddl = ", ".join(f"{k} {v}" for k,v in fields.items())
        cursor.execute(f"CREATE TABLE IF NOT EXISTS lake.silver.{table} ({ddl}, is_deleted BOOLEAN, source_lsn BIGINT, kafka_offset BIGINT, source_ts_ms BIGINT) USING iceberg")
        casts = ", ".join(f"CAST(get_json_object(row_json, '$.{k}') AS {v}) AS {k}" for k,v in fields.items())
        # Single source, fixed one-partition topics. LSN + offset is local to that topic.
        cursor.execute(f"""
        CREATE OR REPLACE TEMP VIEW incoming_{table} AS
        WITH envelopes AS (
          SELECT CASE WHEN get_json_object(payload, '$.op')='d'
                 THEN get_json_object(payload,'$.before') ELSE get_json_object(payload,'$.after') END row_json,
                 get_json_object(payload,'$.op')='d' is_deleted,
                 COALESCE(CAST(get_json_object(payload,'$.source.lsn') AS BIGINT),-1) source_lsn,
                 kafka_offset, CAST(get_json_object(payload,'$.source.ts_ms') AS BIGINT) source_ts_ms
          FROM lake.bronze.cdc_events WHERE topic='cdc.public.{table}'
        ), typed AS (SELECT {casts}, is_deleted, source_lsn, kafka_offset, source_ts_ms FROM envelopes),
        ranked AS (SELECT *, row_number() OVER(PARTITION BY {pk} ORDER BY source_lsn DESC, kafka_offset DESC) rn FROM typed)
        SELECT * FROM ranked WHERE rn=1 AND {pk} IS NOT NULL
        """)
        columns = list(fields) + ["is_deleted", "source_lsn", "kafka_offset", "source_ts_ms"]
        assigns = ", ".join(f"t.{c}=s.{c}" for c in columns)
        cursor.execute(f"""MERGE INTO lake.silver.{table} t USING incoming_{table} s ON t.{pk}=s.{pk}
          WHEN MATCHED AND (s.source_lsn > t.source_lsn OR (s.source_lsn=t.source_lsn AND s.kafka_offset>t.kafka_offset))
          THEN UPDATE SET {assigns}
          WHEN NOT MATCHED THEN INSERT ({','.join(columns)}) VALUES ({','.join('s.'+c for c in columns)})""")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--wait", action="store_true")
    p.add_argument("--sql")
    p.add_argument("--silver-only", action="store_true")
    args=p.parse_args()
    Path(".runtime").mkdir(exist_ok=True)
    lock = open(".runtime/pipeline.lock", "w")
    if not args.sql:
        fcntl.flock(lock, fcntl.LOCK_EX)
    conn=connect(args.wait)
    cursor=conn.cursor()
    if args.sql:
        print(json.dumps(query(cursor,args.sql),default=str,ensure_ascii=False))
        return
    if args.wait:
        for _ in range(90):
            try:
                if query(cursor,"SELECT count(*) FROM lake.bronze.cdc_events")[0][0]>=2:
                    break
            except Exception:
                pass
            time.sleep(2)
        else:
            raise RuntimeError("Initial snapshot rows not visible in Iceberg")
    refresh(cursor)
    cursor.close();conn.close()
    if not args.silver_only:
        subprocess.run(["dbt","build","--project-dir","dbt","--profiles-dir","dbt"],check=True)
    print("Silver refreshed; requested dbt models/tests completed.")

if __name__ == "__main__":
    main()
