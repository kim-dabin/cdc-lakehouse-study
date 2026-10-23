"""Read-only source/Kafka/Bronze comparisons for one common-example refund key."""
import argparse
import json
import os
import re
import time
from pathlib import Path
import psycopg2
from kafka import KafkaConsumer, TopicPartition
from labs.session01.observe import spark


def compact(payload):
    return {'op': payload.get('op'), 'before': payload.get('before'),
            'after': payload.get('after'), 'source_lsn': payload.get('source', {}).get('lsn'),
            'source_ts_ms': payload.get('source', {}).get('ts_ms'),
            'connector_ts_ms': payload.get('ts_ms')}


def kafka_rows(refund_id):
    consumer = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=None,
                             enable_auto_commit=False, request_timeout_ms=10000,
                             api_version_auto_timeout_ms=10000)
    try:
        topic = 'cdc.public.refunds'
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError('Refund topic is absent; inspect connector/topic configuration')
        assigned = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(assigned)
        beginnings = consumer.beginning_offsets(assigned)
        for partition, offset in beginnings.items():
            consumer.seek(partition, offset)
        # Read a bounded snapshot without changing Flink's consumer offsets.
        ends = consumer.end_offsets(assigned)
        result = []
        deadline = time.monotonic() + 30
        while any(consumer.position(p, timeout_ms=1000) < ends[p] for p in assigned):
            if time.monotonic() > deadline:
                raise TimeoutError('Kafka history scan exceeded 30 seconds; restrict retention or inspect offsets')
            for records in consumer.poll(timeout_ms=500, max_records=1000).values():
                for row in records:
                    if row.offset >= ends[TopicPartition(row.topic, row.partition)]:
                        continue
                    key = json.loads(row.key) if row.key else None
                    payload = json.loads(row.value) if row.value else None
                    state = ((payload or {}).get('after') or (payload or {}).get('before') or {})
                    if (key or {}).get('refund_id') != refund_id and state.get('refund_id') != refund_id:
                        continue
                    result.append({'topic': row.topic, 'partition_id': row.partition,
                                   'kafka_offset': row.offset, 'key': key,
                                   'tombstone': row.value is None,
                                   **(compact(payload) if payload else {'op': None})})
        return result
    finally:
        consumer.close()


def source_rows(refund_id):
    with psycopg2.connect(host='source-db', user='source',
                         password=os.environ['SOURCE_PASSWORD'], dbname='app') as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT refund_id,payment_id,amount,status FROM refunds WHERE refund_id=%s', (refund_id,))
            return [dict(zip(['refund_id', 'payment_id', 'amount', 'status'], r)) for r in cur.fetchall()]


def bronze_rows(refund_id):
    rows = spark(f"""SELECT topic,partition_id,kafka_offset,payload,kafka_ts,ingested_at
        FROM lake.bronze.cdc_events
        WHERE topic='cdc.public.refunds'
        AND (get_json_object(payload,'$.after.refund_id')='{refund_id}'
             OR get_json_object(payload,'$.before.refund_id')='{refund_id}')
        ORDER BY partition_id,kafka_offset""")
    return [{'topic': t, 'partition_id': p, 'kafka_offset': o, **compact(json.loads(v)),
             'kafka_ts': kt, 'ingested_at': it} for t, p, o, v, kt, it in rows]


def identity(row):
    return row['topic'], row['partition_id'], row['kafka_offset']


def validate(stage, source, kafka, bronze, refund_id):
    sequence = [('c', None, 20000), ('u', 20000, 30000), ('d', 30000, None)]
    expected = sequence[:{'insert': 1, 'update': 2, 'delete': 3}[stage]]
    kafka_events = [r for r in kafka if not r['tombstone']]
    checks = []
    for op, before, after in expected:
        matches = [r for r in kafka_events if r['op'] == op
                   and (r.get('before') or {}).get('amount') == before
                   and (r.get('after') or {}).get('amount') == after]
        if not matches:
            raise AssertionError(f'Missing Kafka event or incorrect before/after: {op}')
        for row in matches:
            if (row.get('key') or {}).get('refund_id') != refund_id:
                raise AssertionError('Kafka key does not match source PK')
            peers = [b for b in bronze if identity(b) == identity(row)]
            if not peers or any(any(b[field] != row[field] for field in ['op', 'before', 'after', 'source_lsn']) for b in peers):
                raise AssertionError(f'Kafka/Bronze mismatch at {identity(row)}')
            checks.append({'op': op, 'identity': identity(row), 'bronze_copies': len(peers)})
    expected_source_amount = {'insert': 20000, 'update': 30000, 'delete': None}[stage]
    if stage == 'delete':
        if source:
            raise AssertionError('Deleted source row still exists')
    elif len(source) != 1 or source[0]['amount'] != expected_source_amount:
        raise AssertionError(f'Unexpected source state: {source}')
    expected_ops = {op for op, _, _ in expected}
    if any(r['op'] not in expected_ops for r in kafka_events) or len(kafka_events) != len(expected):
        raise AssertionError('Unexpected or repeated business events; inspect rather than discard duplicates')
    if len(bronze) != len(kafka_events):
        raise AssertionError('Unexpected Bronze record count; inspect missing or duplicate locations')
    return checks



def classroom_summary(data):
    """Compact projection of a successful full check; evidence remains complete."""
    stage = data['stage']
    op = {'insert': 'c', 'update': 'u', 'delete': 'd'}[stage]
    row = next(r for r in data['kafka_records'] if r.get('op') == op)
    peer = next(b for b in data['bronze_records'] if identity(b) == identity(row))
    copies = next(c['bronze_copies'] for c in data['checks'] if tuple(c['identity']) == identity(row))
    return {'stage': stage, 'passed': data['passed'], 'source_rows': len(data['source_rows']),
            'refund_id': data['namespace'] + '-r1', 'op': op,
            'before_amount': (row.get('before') or {}).get('amount'),
            'after_amount': (row.get('after') or {}).get('amount'),
            'identity': identity(row), 'source_lsn': row['source_lsn'], 'bronze_copies': copies,
            'history_ops': [b['op'] for b in data['bronze_records']],
            'kafka_tombstones': sum(r['tombstone'] for r in data['kafka_records']),
            'kafka_ts': peer['kafka_ts'], 'ingested_at': peer['ingested_at'],
            'wait_observation': data.get('wait_observation'),
            'scope': data['scope']}


def format_classroom(data):
    topic, partition, offset = data['identity']
    wait = data.get('wait_observation') or {}
    return '\n'.join([
        f"stage={data['stage']}  passed={data['passed']}  source_rows={data['source_rows']}",
        f"refund_id={data['refund_id']}",
        f"op={data['op']}  before_amount={data['before_amount']}  after_amount={data['after_amount']}",
        f"topic={topic}",
        f"partition={partition}  kafka_offset={offset}  bronze_copies={data['bronze_copies']}",
        f"source_lsn={data['source_lsn']}",
        f"history_ops={','.join(data['history_ops'])}  kafka_tombstones={data['kafka_tombstones']}",
        f"kafka_ts={data['kafka_ts']}",
        f"ingested_at={data['ingested_at']}",
        f"wait_observed_seconds={wait.get('wait_observed_seconds', 'not_recorded')}",
        'wait basis: command start to first Bronze observation; not source-commit latency',
        'scope: this synthetic key only; not an exactly-once guarantee',
    ])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('namespace')
    parser.add_argument('action', choices=['kafka', 'source-rows', 'collect-check'])
    parser.add_argument('--stage', choices=['insert', 'update', 'delete'])
    parser.add_argument('--evidence', action='store_true')
    parser.add_argument('--summary', action='store_true', help='Compact classroom output; full evidence is retained')
    args = parser.parse_args()
    if not re.fullmatch(r's01-[a-z0-9-]{3,48}', args.namespace):
        parser.error('Use a fresh namespace beginning with s01-')
    rid = args.namespace + '-r1'
    if args.action == 'source-rows':
        data = {'refund_id': rid, 'source_rows': source_rows(rid)}
    elif args.action == 'kafka':
        data = {'refund_id': rid, 'kafka_records': kafka_rows(rid)}
    else:
        if not args.stage:
            parser.error('--stage is required for collect-check')
        source, kafka, bronze = source_rows(rid), kafka_rows(rid), bronze_rows(rid)
        checks = validate(args.stage, source, kafka, bronze, rid)
        data = {'namespace': args.namespace, 'stage': args.stage, 'source_rows': source,
                'kafka_records': kafka, 'bronze_records': bronze, 'checks': checks, 'passed': True,
                'scope': 'This synthetic key only; not an end-to-end exactly-once or recovery guarantee'}
        event = {'insert':'refund','update':'correction','delete':'delete'}[args.stage]
        wait_path = Path('artifacts/runs/session01') / args.namespace / f'wait-{event}.json'
        data['wait_observation'] = json.loads(wait_path.read_text()) if wait_path.exists() else None
    if args.summary and args.action != 'collect-check':
        parser.error('--summary is supported only for collect-check')
    displayed = classroom_summary(data) if args.summary else data
    print(format_classroom(displayed) if args.summary else json.dumps(displayed, ensure_ascii=False, default=str, indent=2))
    if args.evidence:
        directory = Path('artifacts/runs/session01') / args.namespace
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"collection-{args.stage or args.action}.json").write_text(
            json.dumps(data, ensure_ascii=False, default=str, indent=2))
        if args.action == 'collect-check':
            (directory / f'collection-{args.stage}-summary.json').write_text(
                json.dumps(classroom_summary(data), ensure_ascii=False, default=str, indent=2))


if __name__ == '__main__':
    main()
