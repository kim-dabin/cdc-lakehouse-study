"""Single-node classroom schedule. Runner executes Silver then dbt models/tests."""
import json
import time
from datetime import datetime, timedelta, timezone
from urllib.request import Request, urlopen
from airflow import DAG
from airflow.operators.python import PythonOperator

def refresh():
    with urlopen(Request('http://runner:8080/runs', method='POST'), timeout=10) as r:
        run_id = json.load(r)['id']
    for _ in range(240):
        with urlopen(f'http://runner:8080/runs/{run_id}', timeout=10) as r:
            status = json.load(r)
        if status['state'] == 'success':
            print(status)
            return status
        if status['state'] == 'failed':
            raise RuntimeError(f'Pipeline failed: {status}')
        time.sleep(5)
    raise TimeoutError('Runner still active after 20 minutes; inspect before retry')

with DAG('lakehouse_refresh', start_date=datetime(2026,1,1,tzinfo=timezone.utc),
         schedule='*/5 * * * *', catchup=False, max_active_runs=1,
         is_paused_upon_creation=True, default_args={'retries':0},
         dagrun_timeout=timedelta(minutes=25), tags=['cdc','iceberg','dbt']) as dag:
    PythonOperator(task_id='silver_then_dbt_build', python_callable=refresh)
