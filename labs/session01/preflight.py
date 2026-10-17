"""Instructor-only readiness gate. This checks data queries, not just container state."""
import json
import os
import time
import requests
from scripts.pipeline import connect, query


def flink_json(path, deadline=None):
    timeout=10
    if deadline is not None:
        remaining=deadline-time.monotonic()
        if remaining <= 0:
            raise RuntimeError('Flink did not become checkpoint-ready within 45 seconds')
        timeout=min(timeout,remaining)
    response=requests.get('http://jobmanager:8081'+path,timeout=timeout)
    response.raise_for_status()
    return response.json()


def all_vertices_running(job):
    vertices=job.get('vertices',[])
    return job.get('state')=='RUNNING' and bool(vertices) and all(
        vertex.get('status')=='RUNNING' for vertex in vertices)


def wait_checkpoint_ready(job_id):
    deadline=time.monotonic()+45
    while True:
        job=flink_json('/jobs/'+job_id,deadline)
        checkpoints=flink_json('/jobs/'+job_id+'/checkpoints',deadline)
        if time.monotonic() > deadline:
            raise RuntimeError('Flink did not become checkpoint-ready within 45 seconds')
        if all_vertices_running(job) and checkpoints['counts']['completed'] >= 1:
            return checkpoints
        if job.get('state') in {'FAILED','CANCELED','FINISHED','SUSPENDED'}:
            raise RuntimeError(f'Flink job is not active: {job_id} state={job.get("state")}')
        remaining=deadline-time.monotonic()
        if remaining <= 0:
            raise RuntimeError(
                f'Flink did not become checkpoint-ready within 45 seconds: '
                f'job={job_id} vertices={[v.get("status") for v in job.get("vertices",[])]} '
                f'checkpoint_counts={checkpoints["counts"]}')
        time.sleep(min(1,remaining))


free=os.statvfs('/tmp').f_bavail*os.statvfs('/tmp').f_frsize
if free < 2*1024**3:
    raise RuntimeError(f'VM has only {free/1024**3:.2f} GiB free. Prepare at least 2 GiB before class; 5 GiB is recommended.')
overview=flink_json('/jobs/overview')
active=[j for j in overview['jobs'] if j['name']=='cdc-bronze' and j['state']=='RUNNING']
assert len(active)==1,active
job_id=active[0]['jid']
baseline=wait_checkpoint_ready(job_id)
baseline_failures=baseline['counts']['failed']
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
job=flink_json('/jobs/'+job_id)
checkpoints=flink_json('/jobs/'+job_id+'/checkpoints')
failures_during_check=checkpoints['counts']['failed']-baseline_failures
if failures_during_check != 0:
    raise RuntimeError(
        f'Flink checkpoint failure count changed during readiness checks: '
        f'baseline={baseline_failures} current={checkpoints["counts"]["failed"]} '
        f'delta={failures_during_check}')
if not all_vertices_running(job) or checkpoints['counts']['completed'] < 1:
    raise RuntimeError(
        f'Flink lost checkpoint readiness during data queries: '
        f'job={job_id} state={job.get("state")} checkpoint_counts={checkpoints["counts"]}')
latest_completed=checkpoints.get('latest',{}).get('completed') or {}
print(json.dumps({'ready':True,'vm_free_gib':round(free/1024**3,2),'job_id':job_id,
    'row_counts':result,'checkpoint_counts':checkpoints['counts'],
    'baseline_checkpoint_failures':baseline_failures,
    'checkpoint_failures_during_check':failures_during_check,
    'latest_completed_checkpoint_id':latest_completed.get('id')},indent=2))
