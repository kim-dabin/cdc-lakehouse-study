"""Read source state, checkpoint and Iceberg observations without changing them."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))


def inspect_case(namespace):
    import requests
    from labs.session01.collection import source_rows
    from labs.session02.lab import bronze, spark

    def flink(path):
        response = requests.get('http://jobmanager:8081' + path, timeout=10)
        response.raise_for_status()
        return response.json()

    started = datetime.now(timezone.utc).isoformat()
    jobs = [job for job in flink('/jobs/overview')['jobs']
            if job['name'] == 'cdc-bronze' and job['state'] == 'RUNNING']
    if len(jobs) != 1:
        raise RuntimeError('Expected one RUNNING ingestion job; no restart was requested')
    checkpoints = flink('/jobs/' + jobs[0]['jid'] + '/checkpoints')
    latest = checkpoints.get('latest', {}).get('completed') or {}
    snapshots = spark('SELECT snapshot_id,committed_at,operation FROM lake.bronze.cdc_events.snapshots ORDER BY committed_at DESC LIMIT 1')
    snapshot = dict(zip(['snapshot_id', 'committed_at', 'operation'], snapshots[0])) if snapshots else None
    history = bronze(namespace)
    # Raw payloads remain available through the Session 02 helper when needed.
    history['bronze_records'] = [{k: v for k, v in row.items() if k != 'payload'}
                                 for row in history['bronze_records']]
    source = source_rows(namespace + '-r1')
    return {'case_id': namespace, 'observed_from': started,
            'observed_until': datetime.now(timezone.utc).isoformat(),
            'checkpoint': {'job_id': jobs[0]['jid'], 'job_state': jobs[0]['state'],
                           'counts': checkpoints['counts'],
                           'latest_completed_checkpoint_id': latest.get('id')},
            'iceberg_snapshot': snapshot, 'case_bronze': history,
            'source_refund_rows': source,
            'scope': 'Read-only sequential observations, not one atomic snapshot. A completed checkpoint is not proof of a corresponding Iceberg commit. Checkpoint IDs and snapshot IDs are different identifiers, not a one-to-one mapping. Per-key SQL rows prove current query visibility, not source-commit latency or an exactly-once guarantee.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case_id')
    args = parser.parse_args()
    if not re.fullmatch(r's01-[a-z0-9-]{3,48}', args.case_id):
        parser.error('Use a valid s01- case ID')
    print(json.dumps(inspect_case(args.case_id), ensure_ascii=False, default=str, indent=2))


if __name__ == '__main__':
    main()
