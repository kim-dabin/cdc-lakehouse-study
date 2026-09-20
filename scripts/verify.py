"""Real source mutations -> Iceberg -> marts, with exact expected results."""
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
from kafka import KafkaProducer
from scripts.pipeline import connect, query

RUN = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
ORDER = 'VERIFY-' + RUN
PAYMENT = 'PAY-' + RUN
REFUND = 'REF-' + RUN
results = []

def source(sql, params=()):
    with psycopg2.connect(host='source-db',user='source',password=os.environ['SOURCE_PASSWORD'],dbname='app') as c:
        with c.cursor() as cur:
            cur.execute(sql, params)

def read(sql):
    with connect() as c:
        cur=c.cursor()
        return query(cur,sql)

def wait_event(table, pk, value, op, amount=None):
    condition = f"topic='cdc.public.{table}' AND get_json_object(payload,'$.op')='{op}'"
    part = 'before' if op=='d' else 'after'
    condition += f" AND get_json_object(payload,'$.{part}.{pk}')='{value}'"
    if amount is not None:
        condition += f" AND get_json_object(payload,'$.{part}.amount')='{amount}'"
    for _ in range(90):
        rows=read(f'SELECT payload FROM lake.bronze.cdc_events WHERE {condition} ORDER BY kafka_offset DESC LIMIT 1')
        if rows:
            return rows[0][0]
        time.sleep(2)
    raise AssertionError(f'Bronze event not visible: {condition}')

def build():
    subprocess.run(['python','scripts/pipeline.py'],check=True)

def check(label, expected, started, bronze_at):
    build()
    rows = read(f"SELECT net_paid FROM lake.gold.fct_order_settlement WHERE order_id='{ORDER}'")
    assert rows == [(expected,)], (label, rows, expected)
    results.append({'case':label,'expected_net_paid':expected,'actual_net_paid':rows[0][0],
                    'source_to_bronze_observed_seconds':round(bronze_at-started,3),
                    'source_to_gold_verified_seconds':round(time.monotonic()-started,3)})
    print(json.dumps(results[-1]), flush=True)

def replay(payload):
    producer=KafkaProducer(bootstrap_servers='kafka:9092')
    producer.send('cdc.public.refunds', value=payload.encode()).get(timeout=30)
    producer.flush();producer.close()
    # Wait for the duplicate payload at another Kafka offset to reach Bronze.
    lsn=json.loads(payload)['source']['lsn']
    for _ in range(90):
        count=read(f"SELECT count(*) FROM lake.bronze.cdc_events WHERE topic='cdc.public.refunds' AND get_json_object(payload,'$.source.lsn')='{lsn}' AND get_json_object(payload,'$.after.refund_id')='{REFUND}'")[0][0]
        if count>=2:
            return
        time.sleep(2)
    raise AssertionError('Replayed event not committed to Bronze')

def main():
    started=time.monotonic()
    source('INSERT INTO orders VALUES (%s,%s,%s)',(ORDER,'2026-10-02T10:00:00+09:00','confirmed'))
    source('INSERT INTO payments VALUES (%s,%s,%s,%s)',(PAYMENT,ORDER,100000,'success'))
    wait_event('orders','order_id',ORDER,'c')
    wait_event('payments','payment_id',PAYMENT,'c',100000)
    check('insert',100000,started,time.monotonic())
    started=time.monotonic()
    source('INSERT INTO refunds VALUES (%s,%s,%s,%s)',(REFUND,PAYMENT,20000,'success'))
    original=wait_event('refunds','refund_id',REFUND,'c',20000)
    check('refund_insert',80000,started,time.monotonic())
    started=time.monotonic()
    replay(original)
    check('duplicate_replay',80000,started,time.monotonic())
    started=time.monotonic()
    source('UPDATE refunds SET amount=%s WHERE refund_id=%s',(30000,REFUND))
    wait_event('refunds','refund_id',REFUND,'u',30000)
    check('refund_update',70000,started,time.monotonic())
    started=time.monotonic()
    source('DELETE FROM refunds WHERE refund_id=%s',(REFUND,))
    wait_event('refunds','refund_id',REFUND,'d',30000)
    check('refund_delete',100000,started,time.monotonic())
    # A prior update arrives after the deletion. It must not resurrect the row.
    started=time.monotonic()
    older=read(f"SELECT payload FROM lake.bronze.cdc_events WHERE get_json_object(payload,'$.after.refund_id')='{REFUND}' AND get_json_object(payload,'$.op')='u' LIMIT 1")[0][0]
    replay(older)
    check('old_update_after_delete',100000,started,time.monotonic())
    assert read(f"SELECT is_deleted FROM lake.silver.refunds WHERE refund_id='{REFUND}'")==[(True,)]
    out=Path('artifacts/runs')/f'verification-{RUN}.json'
    out.write_text(json.dumps({'run':RUN,'order_id':ORDER,'results':results},indent=2))
    print(f'PASS: {out}')

if __name__=='__main__':
    main()
