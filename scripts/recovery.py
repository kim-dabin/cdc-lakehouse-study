"""Paired with recovery.sh: checkpoint-based worker restart and result proof."""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import requests
from scripts.verify import source, wait_event, read, build

STATE=Path('.runtime/recovery.json')
def checkpoint_state():
    jobs=requests.get('http://jobmanager:8081/jobs/overview',timeout=10).json()['jobs']
    job=next(j for j in jobs if j['name']=='cdc-bronze' and j['state']=='RUNNING')
    return requests.get(f"http://jobmanager:8081/jobs/{job['jid']}/checkpoints",timeout=10).json()

def main():
    p=argparse.ArgumentParser();p.add_argument('stage',choices=['prepare','mutate','check']);a=p.parse_args()
    if a.stage=='prepare':
        run=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        state={'run':run,'order':'REC-'+run,'payment':'REC-PAY-'+run}
        source('INSERT INTO orders VALUES (%s,%s,%s)',(state['order'],'2026-10-03T10:00:00+09:00','confirmed'))
        source('INSERT INTO payments VALUES (%s,%s,%s,%s)',(state['payment'],state['order'],50000,'success'))
        wait_event('payments','payment_id',state['payment'],'c',50000)
        build()
        state['before']=checkpoint_state()
        STATE.write_text(json.dumps(state))
        print('Initial state committed. Safe to stop this lab TaskManager.')
    elif a.stage=='mutate':
        state=json.loads(STATE.read_text());state['started']=time.monotonic()
        source('UPDATE payments SET amount=%s WHERE payment_id=%s',(70000,state['payment']))
        STATE.write_text(json.dumps(state))
        print('Source update committed while TaskManager is stopped.')
    else:
        state=json.loads(STATE.read_text())
        wait_event('payments','payment_id',state['payment'],'u',70000)
        bronze_elapsed=time.monotonic()-state['started']
        build()
        rows=read(f"SELECT net_paid FROM lake.gold.fct_order_settlement WHERE order_id='{state['order']}'")
        assert rows==[(70000,)],rows
        after=checkpoint_state()
        assert after['counts']['restored'] > state['before']['counts']['restored'],after
        duplicates=read('SELECT topic,partition_id,kafka_offset,count(*) n FROM lake.bronze.cdc_events GROUP BY topic,partition_id,kafka_offset HAVING count(*)>1')
        assert not duplicates, duplicates
        result={'run':state['run'],'expected':70000,'actual':rows[0][0],
                'restored_before':state['before']['counts']['restored'],
                'restored_after':after['counts']['restored'],
                'duplicate_source_offsets':len(duplicates),
                'source_to_bronze_observed_seconds':round(bronze_elapsed,3),
                'source_to_gold_verified_seconds':round(time.monotonic()-state['started'],3),
                'restored_checkpoint':after['latest'].get('restored')}
        out=Path('artifacts/runs')/f"recovery-{state['run']}.json"
        out.write_text(json.dumps(result,indent=2));print(json.dumps(result,indent=2));print(f'PASS: {out}')

if __name__=='__main__':main()
