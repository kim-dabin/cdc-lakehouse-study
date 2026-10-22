"""Session 02 bounded inspections, plus explicit delegation of common-example DML.

No reset, connector mutation, offset commit, schema change or service restart.
Run from the repository root using lab.sh; host Python needs only the stdlib.
"""
import argparse
import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
GLOBAL = ('source-config', 'connector-sanitized', 'snapshot-sample')
READS = ('bronze', 'files', 'parquet', 'kafka', 'parse')
DELEGATED = ('payment', 'refund', 'correction', 'delete', 'source-rows', 'wait', 'collect-check')
SAFE_CONFIG = ('connector.class', 'plugin.name', 'slot.name', 'publication.name', 'topic.prefix',
               'publication.autocreate.mode', 'table.include.list', 'snapshot.mode',
               'tombstones.on.delete', 'key.converter', 'value.converter',
               'key.converter.schemas.enable', 'value.converter.schemas.enable',
               'heartbeat.interval.ms')


def spark(sql):
    from scripts.pipeline import connect, query
    conn = connect(wait=False)
    try:
        cur = conn.cursor()
        try:
            return query(cur, sql)
        finally:
            cur.close()
    finally:
        conn.close()


def source_config():
    import psycopg2
    with psycopg2.connect(host='source-db', user='source', password=os.environ['SOURCE_PASSWORD'],
                          dbname='app', connect_timeout=10, options='-c statement_timeout=10000') as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            def rows(sql):
                cur.execute(sql)
                fields = [d[0] for d in cur.description]
                return [dict(zip(fields, row)) for row in cur.fetchall()]
            settings = rows("SELECT name,setting FROM pg_settings WHERE name IN ('wal_level','max_replication_slots','max_wal_senders') ORDER BY name")
            slots = rows('SELECT slot_name,plugin,slot_type,database,active,restart_lsn,confirmed_flush_lsn FROM pg_replication_slots')
            publications = rows('SELECT pubname,pubinsert,pubupdate,pubdelete,pubtruncate FROM pg_publication')
            tables = rows('SELECT pubname,schemaname,tablename FROM pg_publication_tables ORDER BY pubname,schemaname,tablename')
            identities = rows("SELECT n.nspname AS schemaname,c.relname AS tablename,c.relreplident AS replica_identity_code FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relname IN ('orders','payments','refunds') ORDER BY c.relname")
            primary_keys = rows("SELECT tc.table_name,kcu.column_name,kcu.ordinal_position FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name=kcu.constraint_name AND tc.table_schema=kcu.table_schema WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema='public' AND tc.table_name IN ('orders','payments','refunds') ORDER BY tc.table_name,kcu.ordinal_position")
    return {'settings': settings, 'slots': slots, 'publications': publications,
            'publication_tables': tables, 'replica_identity': identities, 'primary_keys': primary_keys,
            'replica_identity_legend': {'d': 'DEFAULT', 'f': 'FULL', 'i': 'INDEX', 'n': 'NOTHING'},
            'scope': 'Read-only PostgreSQL capture conditions; active slot alone does not prove delivery'}


def connector_config():
    import requests
    base = 'http://connect:8083/connectors/source-postgres'
    response = requests.get(base + '/config', timeout=10)
    response.raise_for_status()
    config = response.json()
    # Explicit allowlist: never return connection passwords or arbitrary settings.
    safe = {key: config[key] for key in SAFE_CONFIG if key in config}
    response = requests.get(base + '/status', timeout=10)
    response.raise_for_status()
    status = response.json()
    return {'connector_name': 'source-postgres', 'config': safe,
            'connector_state': status.get('connector', {}).get('state'),
            'tasks': [{'id': task['id'], 'state': task.get('state')} for task in status.get('tasks', [])],
            'scope': 'Allowlisted configuration and states only; RUNNING is not output verification'}


def bronze(namespace):
    rid = namespace + '-r1'
    fields = ['topic', 'partition_id', 'kafka_offset', 'payload', 'kafka_ts', 'ingested_at', 'parquet_file']
    rows = spark(f"""SELECT topic,partition_id,kafka_offset,payload,kafka_ts,ingested_at,_file
        FROM lake.bronze.cdc_events WHERE topic='cdc.public.refunds'
        AND (get_json_object(payload,'$.before.refund_id')='{rid}'
          OR get_json_object(payload,'$.after.refund_id')='{rid}')
        ORDER BY partition_id,kafka_offset""")
    result = []
    for row in rows:
        data = dict(zip(fields, row))
        payload = json.loads(data['payload'])
        before, after = payload.get('before') or {}, payload.get('after') or {}
        data.update(op=payload.get('op'), before_amount=before.get('amount'),
                    after_amount=after.get('amount'), refund_id=after.get('refund_id') or before.get('refund_id'),
                    source_lsn=payload.get('source', {}).get('lsn'))
        result.append(data)
    return {'namespace': namespace, 'refund_id': rid, 'bronze_records': result,
            'history_ops': [r['op'] for r in result], 'record_count': len(result),
            'scope': 'Current Iceberg table SQL result, filtered by before OR after PK; tombstones excluded by live ingestion'}


def file_plan(namespace):
    data = bronze(namespace)
    paths = sorted({row['parquet_file'] for row in data['bronze_records']})
    for path in paths:
        if not re.fullmatch(r's3://warehouse/bronze/cdc_events/data/[A-Za-z0-9_./=-]+\.parquet', path):
            raise ValueError('Unexpected Bronze data file path')
    if paths:
        quoted = ','.join("'" + path + "'" for path in paths)
        records = spark(f'SELECT file_path,file_format,record_count,file_size_in_bytes FROM lake.bronze.cdc_events.files WHERE file_path IN ({quoted}) ORDER BY file_path')
    else:
        records = []
    data['files'] = [dict(zip(['file_path', 'file_format', 'record_count', 'file_size_in_bytes'], row)) for row in records]
    if len(records) != len(paths):
        raise AssertionError('A selected physical path is absent from the current files metadata')
    return data


def parquet_plan(namespace):
    import psycopg2
    data = file_plan(namespace)
    with psycopg2.connect(host='catalog-db', user='catalog', password=os.environ['CATALOG_PASSWORD'],
                          dbname='catalog', connect_timeout=10, options='-c statement_timeout=10000') as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SELECT metadata_location FROM iceberg_tables WHERE table_namespace='bronze' AND table_name='cdc_events'")
            locations = cur.fetchall()
    if len(locations) != 1:
        raise AssertionError('Expected one current Bronze catalog pointer')
    data['metadata_location'] = locations[0][0]
    return data


def kafka_records(namespace):
    from kafka import KafkaConsumer, TopicPartition
    consumer = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=None, enable_auto_commit=False,
                             request_timeout_ms=10000, api_version_auto_timeout_ms=10000)
    rid = namespace + '-r1'
    try:
        topic = 'cdc.public.refunds'
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError('Refund topic is absent')
        assigned = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(assigned)
        for partition, offset in consumer.beginning_offsets(assigned).items():
            consumer.seek(partition, offset)
        ends = consumer.end_offsets(assigned)
        result, deadline = [], time.monotonic() + 30
        while any(consumer.position(p, timeout_ms=1000) < ends[p] for p in assigned):
            if time.monotonic() > deadline:
                raise TimeoutError('Read-only Kafka history scan exceeded 30 seconds')
            for records in consumer.poll(timeout_ms=500, max_records=1000).values():
                for row in records:
                    if row.offset >= ends[TopicPartition(row.topic, row.partition)]:
                        continue
                    try:
                        key = json.loads(row.key) if row.key else None
                    except (ValueError, UnicodeError):
                        key = None
                    target_key = isinstance(key, dict) and key.get('refund_id') == rid
                    # Retain a malformed key when the valid envelope still identifies this case.
                    if not target_key:
                        try:
                            payload = json.loads(row.value) if row.value else None
                            state = (payload.get('after') or payload.get('before') or {}) if isinstance(payload, dict) else {}
                            target_key = isinstance(state, dict) and state.get('refund_id') == rid
                        except (ValueError, UnicodeError):
                            target_key = False
                    if not target_key:
                        continue
                    item = {'metadata': {'topic': row.topic, 'partition_id': row.partition,
                                         'kafka_offset': row.offset, 'kafka_ts_ms': row.timestamp}}
                    for field, raw in (('key', row.key), ('value', row.value)):
                        try:
                            item[field] = None if raw is None else raw.decode('utf-8')
                        except UnicodeError:
                            # Preserve undecodable bytes rather than replacing them or calling them tombstones.
                            item[field] = None
                            item[field + '_wire_base64'] = base64.b64encode(raw).decode('ascii')
                    result.append(item)
        return {'namespace': namespace, 'refund_id': rid, 'origin': 'live_kafka', 'kafka_records': result,
                'scope': 'Bounded retained-log read, no group ID and no offset commit; not a connector reset'}
    finally:
        consumer.close()


def snapshot_sample():
    rows = spark("""SELECT topic,partition_id,kafka_offset,payload FROM lake.bronze.cdc_events
        WHERE get_json_object(payload,'$.op')='r' ORDER BY topic,partition_id,kafka_offset LIMIT 2""")
    return {'origin': 'live_bronze', 'samples': [dict(zip(['topic', 'partition_id', 'kafka_offset', 'payload'], row)) for row in rows],
            'sample_found': bool(rows), 'scope': 'Retained initial snapshot r examples; no new snapshot is triggered. If absent, use the explicitly synthetic parser fixture.'}


def parse_record(row):
    from labs.session02.event_parser import parse_event
    def raw(field):
        encoded = row.get(field + '_wire_base64')
        return base64.b64decode(encoded) if encoded is not None else row[field]
    return parse_event(raw('key'), raw('value'), row['metadata'])


def inside(action, namespace):
    if action == 'source-config':
        return source_config()
    if action == 'connector-sanitized':
        return connector_config()
    if action == 'snapshot-sample':
        return snapshot_sample()
    if action == 'bronze':
        return bronze(namespace)
    if action in ('files', 'parquet-plan'):
        return parquet_plan(namespace) if action == 'parquet-plan' else file_plan(namespace)
    if action in ('kafka', 'parse'):
        data = kafka_records(namespace)
        if action == 'parse':
            data['parsed_events'] = [parse_record(row) for row in data.pop('kafka_records')]
        return data
    raise ValueError('Unsupported action')


def runner(action, namespace):
    command = ['./scripts/lab.sh', 'exec', '-T', 'runner', 'python', 'labs/session02/lab.py', '--inside-runner', action]
    if namespace:
        command.append(namespace)
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=120, check=True)
    return json.loads(result.stdout)


def physical_parquet(namespace):
    plan = runner('parquet-plan', namespace)
    if not plan['files']:
        return {**plan, 'passed': False, 'reason': 'No rows for this key; inspect the key or create a fresh common example', 'physical_files': []}
    if len(plan['files']) > 20:
        raise ValueError('Teaching inspection is limited to 20 selected files')
    source = ROOT.joinpath('labs/session02/ReadParquet.java').read_text()
    # The shell program is constant; all data values are positional arguments, not interpolation.
    script = 'set -eu; work=$(mktemp -d /tmp/session02-parquet.XXXXXX); trap \'rm -rf "$work"\' EXIT; cat > "$work/ReadParquet.java"; java --class-path "/opt/spark/jars/*" "$work/ReadParquet.java" "$@"'
    command = ['./scripts/lab.sh', 'exec', '-T', 'spark', 'sh', '-c', script, 'session02',
               plan['metadata_location'], plan['refund_id']] + [row['file_path'] for row in plan['files']]
    result = subprocess.run(command, cwd=ROOT, input=source, capture_output=True, text=True, timeout=120, check=True)
    physical = json.loads(result.stdout)
    expected = {(row['topic'], row['partition_id'], row['kafka_offset'], row['payload']) for row in plan['bronze_records']}
    actual, total_matches = set(), 0
    metadata = {row['file_path']: row for row in plan['files']}
    for file in physical['physical_files']:
        info = metadata[file['file_path']]
        if file['byte_count'] != info['file_size_in_bytes'] or file['decoded_record_count'] != info['record_count']:
            raise AssertionError('Physical file length/row count differs from current files metadata')
        if file['magic_start'] != 'PAR1' or file['magic_end'] != 'PAR1' or info['file_format'] != 'PARQUET':
            raise AssertionError('Physical file is not the expected Parquet format')
        for row in file['matching_records']:
            total_matches += 1
            actual.add((row['topic'], row['partition_id'], row['kafka_offset'], row['payload']))
    if actual != expected or total_matches != len(plan['bronze_records']):
        raise AssertionError('Physically decoded records do not match direct Bronze SQL rows')
    ops = plan['history_ops']
    complete = ops == ['c', 'u', 'd']
    return {**plan, **physical, 'passed': True, 'sql_file_records_match': True,
            'matching_record_count': total_matches, 'three_event_history_complete': complete,
            'scope': 'Selected current append-only Bronze files were read as bytes and decoded; exact payload+Kafka identity matched SQL. This is not a general Iceberg delete-file reader or exactly-once guarantee.'}


def classroom_summary(action, data):
    """Keep raw payloads in evidence, with a shorter teaching projection on screen."""
    output = dict(data)
    if action in ('bronze', 'files', 'parquet'):
        output['bronze_records'] = [{key: value for key, value in row.items() if key != 'payload'} for row in data['bronze_records']]
    if action == 'parquet':
        output['physical_files'] = []
        for file in data['physical_files']:
            item = dict(file)
            item['matching_records'] = []
            for row in file['matching_records']:
                envelope = json.loads(row['payload'])
                item['matching_records'].append({key: value for key, value in row.items() if key != 'payload'} | {
                    'op': envelope.get('op'), 'before_amount': (envelope.get('before') or {}).get('amount'),
                    'after_amount': (envelope.get('after') or {}).get('amount')})
            output['physical_files'].append(item)
    if action == 'kafka':
        output['kafka_records'] = []
        for row in data['kafka_records']:
            event = parse_record(row)
            output['kafka_records'].append({'metadata': event['metadata'], 'key': event.get('pk'),
                                          'kind': event['kind'], 'op': event.get('op'),
                                          'error_code': event.get('error_code'),
                                          'before_amount': (event.get('before') or {}).get('amount'),
                                          'after_amount': (event.get('after') or {}).get('amount')})
    if action == 'parse':
        output['parsed_events'] = [{key: value for key, value in row.items() if key not in ('before', 'after', 'row')} |
                                   {'event_amount': (row.get('row') or {}).get('amount')} for row in data['parsed_events']]
    return output


def main():
    os.chdir(ROOT)
    if '--inside-runner' in sys.argv:
        index = sys.argv.index('--inside-runner')
        action = sys.argv[index + 1]
        namespace = sys.argv[index + 2] if len(sys.argv) > index + 2 else None
        print(json.dumps(inside(action, namespace), ensure_ascii=False, default=str))
        return
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case_or_action')
    parser.add_argument('action', nargs='?')
    parser.add_argument('--evidence', action='store_true')
    parser.add_argument('--summary', action='store_true', help='Short classroom projection; evidence retains raw payloads')
    args, remainder = parser.parse_known_args()
    if args.case_or_action in GLOBAL:
        if args.action or remainder:
            parser.error('Global inspection accepts only --evidence/--summary')
        action, namespace = args.case_or_action, None
    else:
        namespace, action = args.case_or_action, args.action
        if not re.fullmatch(r's01-[a-z0-9-]{3,48}', namespace):
            parser.error('Use an s01- namespace, such as s01-s02-live-1022120000')
        if action not in READS + DELEGATED:
            parser.error('Unknown action')
    if action in DELEGATED:
        command = ['./labs/session01/lab.sh', namespace, action] + remainder
        if args.evidence:
            command.append('--evidence')
        if args.summary:
            command.append('--summary')
        # Only explicitly requested DML actions write source rows; everything else observes.
        raise SystemExit(subprocess.run(command, cwd=ROOT).returncode)
    if remainder:
        parser.error('Read-only inspection accepts only --evidence/--summary')
    data = physical_parquet(namespace) if action == 'parquet' else runner(action, namespace)
    data['observed_at'] = datetime.now(timezone.utc).isoformat()
    if args.evidence:
        directory = ROOT / 'artifacts/runs/session02' / (namespace or 'global')
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f'{action}.json'
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + '\n')
        data['evidence_path'] = str(path.relative_to(ROOT))
    print(json.dumps(classroom_summary(action, data) if args.summary else data, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    try:
        main()
    except subprocess.CalledProcessError as error:
        # Do not expose arbitrary dependency stderr/configuration in a public lesson log.
        print(json.dumps({'passed': False, 'error_type': type(error).__name__,
                          'message': 'Inspection subprocess failed; review its service logs privately (credentials are not printed).'}), file=sys.stderr)
        raise SystemExit(1)
