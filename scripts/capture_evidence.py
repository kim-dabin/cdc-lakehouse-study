"""Read-only live evidence; credentials and connector config are intentionally excluded."""
import json
from datetime import datetime,timezone
from pathlib import Path
import requests
from scripts.verify import read

jobs=requests.get('http://jobmanager:8081/jobs/overview',timeout=10).json()['jobs']
job=next(j for j in jobs if j['name']=='cdc-bronze' and j['state']=='RUNNING')
checkpoints=requests.get(f"http://jobmanager:8081/jobs/{job['jid']}/checkpoints",timeout=10).json()
connect=requests.get('http://connect:8083/connectors/source-postgres/status',timeout=10).json()
duplicates=read('SELECT topic,partition_id,kafka_offset,count(*) FROM lake.bronze.cdc_events GROUP BY topic,partition_id,kafka_offset HAVING count(*)>1')
assert not duplicates,duplicates
assert all(t['state']=='RUNNING' for t in connect['tasks']),connect
result=json.loads(Path('dbt/target/run_results.json').read_text())
assert all(r['status'] in ('pass','success') for r in result['results']),result['results']
evidence={
 'captured_at':datetime.now(timezone.utc).isoformat(),
 'flink':{'job_id':job['jid'],'state':job['state'],'checkpoints':checkpoints['counts'],'latest_restored':checkpoints['latest'].get('restored')},
 'connector_state':connect['connector']['state'],'connector_task_states':[t['state'] for t in connect['tasks']],
 'bronze_rows':read('SELECT count(*) FROM lake.bronze.cdc_events')[0][0],
 'duplicate_topic_partition_offsets':len(duplicates),
 'daily_mart':read('SELECT order_date,order_count,paid,refunded,net_paid FROM lake.gold.mart_daily_settlement ORDER BY order_date'),
 'dbt':[{'node':r['unique_id'],'status':r['status']} for r in result['results']],
}
path=Path('artifacts/runs/runtime-evidence.json');path.write_text(json.dumps(evidence,indent=2,default=str))
print(json.dumps(evidence,indent=2,default=str))
