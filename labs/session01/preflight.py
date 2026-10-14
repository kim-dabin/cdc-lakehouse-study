"""Instructor-only readiness gate. This checks data queries, not just container state."""
import json
import os
import requests
from scripts.pipeline import connect, query

free=os.statvfs('/tmp').f_bavail*os.statvfs('/tmp').f_frsize
if free < 2*1024**3:
    raise RuntimeError(f'VM has only {free/1024**3:.2f} GiB free. Prepare at least 2 GiB before class; 5 GiB is recommended.')
response=requests.get('http://jobmanager:8081/jobs/overview',timeout=10);response.raise_for_status()
active=[j for j in response.json()['jobs'] if j['name']=='cdc-bronze' and j['state']=='RUNNING']
assert len(active)==1,active
response=requests.get('http://connect:8083/connectors/source-postgres/status',timeout=10);response.raise_for_status()
connector=response.json()
assert connector['connector']['state']=='RUNNING' and connector['tasks'] and all(t['state']=='RUNNING' for t in connector['tasks']),connector
conn=connect(wait=True)
try:
    cur=conn.cursor()
    result={table:query(cur,f'SELECT count(*) FROM {table}')[0][0] for table in
      ['lake.bronze.cdc_events','lake.silver.payments','lake.gold.fct_order_settlement']}
    cur.close()
finally:
    conn.close()
print(json.dumps({'ready':True,'vm_free_gib':round(free/1024**3,2),'job_id':active[0]['jid'],'row_counts':result},indent=2))
