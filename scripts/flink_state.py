"""Savepoint-aware stop and safe submission preparation for this fixed SQL job."""
import argparse
import json
import time
from pathlib import Path
import requests

BASE='http://jobmanager:8081'
STATE=Path('.runtime/flink-state.json')

def jobs():
    r=requests.get(BASE+'/jobs/overview',timeout=10);r.raise_for_status()
    return r.json()['jobs']

def active_jobs():
    return [j for j in jobs() if j['name']=='cdc-bronze' and j['state'] in ('RUNNING','RESTARTING','CREATED')]

def sink_tables(job_id):
    """Iceberg sink table names in a job graph, e.g. {'lake.bronze.orders'}."""
    r=requests.get(f'{BASE}/jobs/{job_id}',timeout=10);r.raise_for_status()
    names=set()
    for vertex in r.json().get('vertices',[]):
        for part in vertex['name'].split('IcebergSink ')[1:]:
            names.add(part.split(':')[0].strip())
    return names

def stop_with_savepoint():
    active=active_jobs()
    if len(active)!=1:raise RuntimeError(f'Expected one active cdc-bronze job, found {len(active)}')
    job_id=active[0]['jid']
    r=requests.post(f'{BASE}/jobs/{job_id}/stop',json={'drain':False,'targetDirectory':'file:///opt/flink/state/savepoints'},timeout=30)
    r.raise_for_status();request_id=r.json()['request-id']
    for _ in range(120):
        r=requests.get(f'{BASE}/jobs/{job_id}/savepoints/{request_id}',timeout=10);r.raise_for_status();data=r.json()
        if data['status']['id']=='COMPLETED':
            location=data.get('operation',{}).get('location')
            if not location:raise RuntimeError(data)
            STATE.write_text(json.dumps({'job_id':job_id,'savepoint':location}))
            print(f'Stopped with savepoint: {location}');return job_id,location
        time.sleep(1)
    raise TimeoutError('Savepoint still in progress; inspect Flink before stopping containers')

def prepare(fresh_state=False):
    if active_jobs():raise RuntimeError('An active ingestion job exists; refusing duplicate submission')
    state=json.loads(STATE.read_text()) if STATE.exists() else {}
    if state and not state.get('savepoint'):
        raise RuntimeError('Previous job has no saved stop point. Recover explicitly; refusing a submission without restored state.')
    sql=Path('.runtime/ingest.sql').read_text()
    if fresh_state:
        # Planned job-graph change only (scripts/migrate-bronze-per-topic.sh). The previous job
        # was stopped with a savepoint, so its Kafka group offsets match its last Iceberg commit;
        # the new graph cannot restore that state and resumes from the committed group offsets.
        if not state.get('savepoint'):
            raise RuntimeError('--fresh-state requires a job that was stopped with a savepoint first')
    elif state.get('savepoint'):
        location=state['savepoint'].replace("'","''")
        sql=f"SET 'execution.savepoint.path' = '{location}';\n"+sql
    Path('.runtime/submit.sql').write_text(sql)

def check():
    for _ in range(90):
        active=[j for j in jobs() if j['name']=='cdc-bronze' and j['state']=='RUNNING']
        if len(active)==1:
            # Consume the saved point. An unplanned full cluster loss must not silently replay it.
            STATE.write_text(json.dumps({'job_id':active[0]['jid']}))
            print(f"RUNNING: {active[0]['jid']} (data validation is still required)");return active[0]['jid']
        time.sleep(1)
    raise RuntimeError('Submitted ingestion job did not reach RUNNING')

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['stop','prepare','check'])
    p.add_argument('--fresh-state',action='store_true',help='prepare only: submit without restoring the saved state (planned job-graph change)')
    a=p.parse_args()
    if a.action=='stop':stop_with_savepoint()
    elif a.action=='prepare':prepare(a.fresh_state)
    else:check()

if __name__=='__main__':main()
