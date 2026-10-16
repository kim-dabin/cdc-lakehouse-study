"""Session 01 instructor observations. Read-only except an optional evidence file."""
import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
import os
import psycopg2
from scripts.pipeline import connect, query


def spark(sql):
    conn=connect(wait=True)
    try:
        cur=conn.cursor()
        try:
            return query(cur, sql)
        finally:
            cur.close()
    finally:
        conn.close()


def source(namespace):
    conn=psycopg2.connect(host='source-db',user='source',password=os.environ['SOURCE_PASSWORD'],dbname='app')
    try:
        cur=conn.cursor()
        cur.execute('''SELECT p.amount, coalesce((SELECT sum(r.amount) FROM refunds r
                       WHERE r.payment_id=p.payment_id AND r.status='success'),0)
                       FROM payments p WHERE p.payment_id=%s''',(namespace+'-p1',))
        return [{'paid':int(p),'refunded':int(r),'net_paid':int(p-r)} for p,r in cur.fetchall()]
    finally:
        conn.close()


def bronze_sql(namespace):
    return f"""SELECT topic,partition_id,kafka_offset,
        get_json_object(payload,'$.op') op,
        get_json_object(payload,'$.before.amount') before_amount,
        get_json_object(payload,'$.after.amount') after_amount,
        get_json_object(payload,'$.source.lsn') source_lsn,
        kafka_ts,ingested_at
        FROM lake.bronze.cdc_events
        WHERE get_json_object(payload,'$.after.order_id')='{namespace}-o1'
           OR get_json_object(payload,'$.before.order_id')='{namespace}-o1'
           OR get_json_object(payload,'$.after.refund_id')='{namespace}-r1'
           OR get_json_object(payload,'$.before.refund_id')='{namespace}-r1'
        ORDER BY topic,kafka_offset"""


def layers(namespace):
    return {
      'namespace':namespace,
      'source':source(namespace),
      'silver_payment':spark(f"SELECT payment_id,amount,is_deleted,source_lsn FROM lake.silver.payments WHERE payment_id='{namespace}-p1'"),
      'silver_refund':spark(f"SELECT refund_id,amount,is_deleted,source_lsn FROM lake.silver.refunds WHERE refund_id='{namespace}-r1'"),
      'gold_order':spark(f"SELECT order_id,order_date,paid,refunded,net_paid,mart_built_at FROM lake.gold.fct_order_settlement WHERE order_id='{namespace}-o1'"),
    }


def wait(namespace,event):
    table,pk,value,op,amount={
      'payment':('payments','payment_id',namespace+'-p1','c',100000),
      'refund':('refunds','refund_id',namespace+'-r1','c',20000),
      'correction':('refunds','refund_id',namespace+'-r1','u',30000),
      'delete':('refunds','refund_id',namespace+'-r1','d',30000),
    }[event]
    part='before' if op=='d' else 'after'
    sql=f"""SELECT count(*) FROM lake.bronze.cdc_events WHERE topic='cdc.public.{table}'
        AND get_json_object(payload,'$.op')='{op}'
        AND get_json_object(payload,'$.{part}.{pk}')='{value}'
        AND get_json_object(payload,'$.{part}.amount')='{amount}'"""
    started=time.monotonic()
    wait_started_at=datetime.now(timezone.utc).isoformat()
    for _ in range(60):
        if spark(sql)[0][0]>0:
            if event=='payment':
                if spark(f"SELECT count(*) FROM lake.bronze.cdc_events WHERE topic='cdc.public.orders' AND get_json_object(payload,'$.after.order_id')='{namespace}-o1'")[0][0]==0:
                    time.sleep(2);continue
            return {'namespace':namespace,'event':event,'bronze_visible':True,
                    'wait_observed_seconds':round(time.monotonic()-started,2),
                    'wait_started_at':wait_started_at,
                    'bronze_observed_at':datetime.now(timezone.utc).isoformat()}
        time.sleep(2)
    raise RuntimeError(f'Event not visible in Bronze within polling budget: {event}; inspect CDC/Flink before refreshing Gold')


def main():
    p=argparse.ArgumentParser();p.add_argument('namespace');p.add_argument('action',choices=['bronze','layers','wait','check'])
    p.add_argument('--event',choices=['payment','refund','correction','delete']);p.add_argument('--expected',type=int)
    p.add_argument('--evidence',action='store_true');a=p.parse_args()
    if not re.fullmatch(r's01-[a-z0-9-]{3,48}',a.namespace):
        p.error('namespace must start with s01- and contain lowercase letters, numbers or hyphens only')
    if a.action=='wait':
        if not a.event:p.error('--event is required')
        data=wait(a.namespace,a.event)
        print(json.dumps(data,ensure_ascii=False,indent=2))
        if a.evidence:
            out=Path('artifacts/runs/session01')/a.namespace
            out.mkdir(parents=True,exist_ok=True)
            (out/f'wait-{a.event}.json').write_text(json.dumps(data,indent=2))
        return
    data=spark(bronze_sql(a.namespace)) if a.action=='bronze' else layers(a.namespace)
    if a.action=='check':
        if a.expected is None:p.error('--expected is required')
        assert len(data['source'])==1 and data['source'][0]['net_paid']==a.expected,data
        assert len(data['gold_order'])==1 and data['gold_order'][0][4]==a.expected,data
        data['expected']=a.expected;data['passed']=True
    print(json.dumps(data,default=str,ensure_ascii=False,indent=2))
    if a.evidence:
        out=Path('artifacts/runs/session01')/a.namespace
        out.mkdir(parents=True,exist_ok=True)
        (out/f'{a.action}-{a.expected}.json').write_text(json.dumps(data,default=str,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
