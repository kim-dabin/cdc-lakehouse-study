"""Move the shared Bronze from lake.bronze.cdc_events to one table per Kafka topic.

Run through scripts/migrate-bronze-per-topic.sh (host). Each stage runs in the runner
container and is safe to repeat:

  stop          stop the single-table cdc-bronze job with a savepoint (no-op when the
                per-topic job already runs or the job was already stopped with a savepoint)
  backfill      create lake.bronze.{orders,payments,refunds} if missing, copy each topic
                from lake.bronze.cdc_events only into an empty table, then verify counts,
                offsets and content per topic and check that the Kafka group offsets
                continue exactly after the copied history
  needs-submit  exit 10 when the per-topic job must be submitted, 0 when it already runs
  summary       wait for the per-topic job and its first checkpoint, print a summary

lake.bronze.cdc_events is never modified or dropped. Any failed check stops the
migration with a message; nothing is retried or deleted automatically.
"""
import argparse
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import flink_state  # noqa: E402

LEGACY = 'lake.bronze.cdc_events'
TABLES = ('orders', 'payments', 'refunds')
TOPIC = {table: f'cdc.public.{table}' for table in TABLES}
PER_TOPIC_SINKS = {f'lake.bronze.{table}' for table in TABLES}
GROUP = 'flink-cdc-bronze'
RECORD = Path('.runtime/bronze-migration.json')
NEEDS_SUBMIT = 10


class Refused(Exception):
    """A check failed; the operator must inspect before anything else happens."""


def now():
    return datetime.now(timezone.utc).isoformat()


def load():
    return json.loads(RECORD.read_text()) if RECORD.exists() else {}


def save(record):
    RECORD.parent.mkdir(exist_ok=True)
    RECORD.write_text(json.dumps(record, indent=2, default=str))


def spark(sql):
    from scripts.pipeline import connect, query
    conn = connect(wait=True)
    try:
        cursor = conn.cursor()
        try:
            return query(cursor, sql)
        finally:
            cursor.close()
    finally:
        conn.close()


def bronze_tables():
    return {row[1] for row in spark('SHOW TABLES IN lake.bronze')}


def job_layout():
    """('per_topic'|'single'|'none', job_id). Anything else is refused."""
    active = flink_state.active_jobs()
    if not active:
        return 'none', None
    if len(active) > 1:
        raise Refused(f'{len(active)} active cdc-bronze jobs; inspect Flink first')
    job_id = active[0]['jid']
    sinks = flink_state.sink_tables(job_id)
    if sinks == PER_TOPIC_SINKS:
        return 'per_topic', job_id
    if sinks == {LEGACY}:
        return 'single', job_id
    raise Refused(f'cdc-bronze {job_id} writes to unexpected tables {sorted(sinks)}; inspect Flink first')


def stop():
    record = load()
    layout, job_id = job_layout()
    if layout == 'per_topic':
        print(f'Per-topic cdc-bronze job {job_id} is already running; nothing to stop.')
        return
    if layout == 'single':
        record.update(started_at=now(), stop_requested_at=now(), legacy_job_id=job_id)
        save(record)
        _, savepoint = flink_state.stop_with_savepoint()
        record.update(legacy_savepoint=savepoint, stopped_at=now())
        save(record)
        return
    if record.get('completed'):
        raise Refused('Migration already completed and no cdc-bronze job is running. '
                      'Start it normally (./scripts/bootstrap.sh --no-build) so it restores its own savepoint.')
    if 'cdc_events' not in bronze_tables():
        raise Refused(f'{LEGACY} does not exist: this environment already uses per-topic tables. '
                      'Start the job normally with ./scripts/bootstrap.sh --no-build.')
    state = json.loads(flink_state.STATE.read_text()) if flink_state.STATE.exists() else {}
    try:
        stopped_sinks = flink_state.sink_tables(state['job_id']) if state.get('job_id') else set()
    except Exception:
        stopped_sinks = set()  # the JobManager no longer lists that job
    if stopped_sinks == PER_TOPIC_SINKS:
        raise Refused('The stopped job already wrote per-topic tables; restore it normally with ./scripts/bootstrap.sh --no-build.')
    if not state.get('savepoint'):
        raise Refused('No cdc-bronze job is running and no savepoint was recorded. '
                      'Without a clean stop the Kafka group offsets may not match Bronze; inspect Flink first.')
    if record.get('legacy_savepoint') not in (None, state['savepoint']):
        raise Refused('The recorded savepoint differs from the one in .runtime/flink-state.json; inspect before continuing')
    record.setdefault('started_at', now())
    record.update(legacy_savepoint=state['savepoint'])
    save(record)
    print(f'cdc-bronze is already stopped with savepoint {state["savepoint"]}; continuing.')


def stats(source, topic, max_offsets=None):
    """Per partition: rows, min/max offset, distinct offsets, content checksum."""
    where = f"topic='{topic}'"
    if max_offsets:
        where += ' AND (' + ' OR '.join(f'(partition_id={p} AND kafka_offset<={o})'
                                         for p, o in sorted(max_offsets.items())) + ')'
    rows = spark(f"""SELECT partition_id, count(*), min(kafka_offset), max(kafka_offset),
        count(DISTINCT kafka_offset),
        CAST(sum(CAST(xxhash64(payload, topic, partition_id, kafka_offset, kafka_ts, ingested_at) AS DECIMAL(38,0))) AS STRING)
        FROM {source} WHERE {where} GROUP BY partition_id ORDER BY partition_id""")
    return {int(r[0]): {'rows': r[1], 'min_offset': r[2], 'max_offset': r[3],
                        'distinct_offsets': r[4], 'checksum': r[5]} for r in rows}


def kafka_alignment(legacy):
    """The next offset the group resumes from must follow the copied history exactly.

    Offsets between the last copied record and the committed offset may only be
    tombstones (null values), which Bronze skips by design.
    """
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
    admin = KafkaAdminClient(bootstrap_servers='kafka:9092', request_timeout_ms=10000)
    try:
        committed = admin.list_consumer_group_offsets(GROUP)
    finally:
        admin.close()
    consumer = KafkaConsumer(bootstrap_servers='kafka:9092', group_id=None, enable_auto_commit=False,
                             request_timeout_ms=10000, api_version_auto_timeout_ms=10000)
    result, problems, gaps = [], [], {}
    try:
        for table in TABLES:
            topic = TOPIC[table]
            for partition in sorted(consumer.partitions_for_topic(topic) or set()):
                tp = TopicPartition(topic, partition)
                end = consumer.end_offsets([tp])[tp]
                begin = consumer.beginning_offsets([tp])[tp]
                last = legacy[table].get(partition, {}).get('max_offset')
                meta = committed.get(tp)
                group = meta.offset if meta is not None and meta.offset >= 0 else None
                row = {'topic': topic, 'partition': partition, 'log_start': begin, 'log_end': end,
                       'bronze_last_offset': last, 'group_committed': group}
                expected_next = (last + 1) if last is not None else None
                if group is None:
                    if last is not None:
                        problems.append(f'{topic}[{partition}]: group {GROUP} has no committed offset; '
                                        f'the new job would re-read from the earliest offset {begin}')
                elif expected_next is not None and group < expected_next:
                    problems.append(f'{topic}[{partition}]: committed {group} < next uncopied offset {expected_next}; '
                                    f'{expected_next - group} record(s) would be appended twice')
                elif group > end:
                    problems.append(f'{topic}[{partition}]: committed {group} is beyond the log end {end}')
                else:
                    start = expected_next if expected_next is not None else begin
                    if group > start:
                        gaps[tp] = (start, group, row)
                    row['records_between'] = 0
                    row['tombstones_between'] = 0
                result.append(row)
        if gaps:
            # Read only the offsets between the copied history and the committed offset.
            consumer.assign(list(gaps))
            for tp, (start, _, _) in gaps.items():
                consumer.seek(tp, start)
            deadline = time.monotonic() + 30
            while any(consumer.position(tp) < stop for tp, (_, stop, _) in gaps.items()):
                if time.monotonic() > deadline:
                    raise Refused('Reading the Kafka offsets after the copied history timed out')
                for tp, records in consumer.poll(timeout_ms=500).items():
                    start, stop, row = gaps[tp]
                    for record in records:
                        if start <= record.offset < stop:
                            row['records_between'] += 1
                            if record.value is None:
                                row['tombstones_between'] += 1
                            else:
                                problems.append(f'{tp.topic}[{tp.partition}]: offset {record.offset} is after the copied '
                                                f'history but before the committed offset {stop}; it would be missing from Bronze')
    finally:
        consumer.close()
    return result, problems


def remediation(alignment):
    lines = ['If the copied history is correct, align the group (the job must be stopped) and rerun:']
    for row in alignment:
        if row['bronze_last_offset'] is not None:
            lines.append(f"  ./scripts/lab.sh exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 "
                         f"--group {GROUP} --topic {row['topic']}:{row['partition']} --reset-offsets "
                         f"--to-offset <first offset after {row['bronze_last_offset']} that is not a tombstone> --execute")
    return '\n'.join(lines)


def backfill():
    record = load()
    layout, job_id = job_layout()
    if layout == 'single':
        raise Refused('The single-table job is still running; run the stop stage first')
    tables = bronze_tables()
    if 'cdc_events' not in tables:
        if layout == 'per_topic' or not record:
            print(f'{LEGACY} does not exist: a fresh per-topic environment; nothing to copy.')
            record.update(backfill={'skipped': 'no legacy table'}, verified_at=now())
            save(record)
            return
        raise Refused(f'{LEGACY} is missing although a migration was started; inspect the catalog')
    Path('.runtime').mkdir(exist_ok=True)
    lock = open('.runtime/pipeline.lock', 'w')
    fcntl.flock(lock, fcntl.LOCK_EX)  # no Silver refresh reads half-built tables meanwhile
    try:
        legacy = {table: stats(LEGACY, TOPIC[table]) for table in TABLES}
        other = spark(f"SELECT topic, count(*) FROM {LEGACY} WHERE topic NOT IN "
                      f"({','.join(repr(t) for t in TOPIC.values())}) GROUP BY topic")
        if other:
            raise Refused(f'{LEGACY} holds topics outside the three tables: {other}')
        report = {}
        for table in TABLES:
            target = f'lake.bronze.{table}'
            spark(f"""CREATE TABLE IF NOT EXISTS {target} (payload STRING, topic STRING, partition_id INT,
                kafka_offset BIGINT, kafka_ts TIMESTAMP, ingested_at TIMESTAMP) USING iceberg
                TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')""")
            existing = spark(f'SELECT count(*) FROM {target}')[0][0]
            copied = 0
            if existing == 0 and legacy[table]:
                if layout == 'per_topic':
                    raise Refused(f'{target} is empty while the per-topic job runs; inspect before copying')
                # One INSERT is one Iceberg commit: the copy is all or nothing.
                spark(f"""INSERT INTO {target}
                    SELECT payload, topic, partition_id, kafka_offset, kafka_ts, ingested_at
                    FROM {LEGACY} WHERE topic='{TOPIC[table]}' ORDER BY partition_id, kafka_offset""")
                copied = sum(v['rows'] for v in legacy[table].values())
            elif existing:
                print(f'{target} already has {existing} rows; not copying again, verifying only.')
            wrong = spark(f"SELECT topic, count(*) FROM {target} WHERE topic IS NULL OR topic<>'{TOPIC[table]}' GROUP BY topic")
            if wrong:
                raise Refused(f'{target} holds rows of another topic: {wrong}')
            maxima = {p: v['max_offset'] for p, v in legacy[table].items()}
            copy_stats = stats(target, TOPIC[table], maxima) if maxima else {}
            if copy_stats != legacy[table]:
                raise Refused(f'{target} history differs from {LEGACY} for {TOPIC[table]}:\n'
                              f'  legacy={legacy[table]}\n  target={copy_stats}')
            newer = spark(f"""SELECT count(*), count(DISTINCT partition_id, kafka_offset) FROM {target}
                WHERE NOT ({' OR '.join(f'(partition_id={p} AND kafka_offset<={o})' for p, o in maxima.items()) or 'false'})""")[0]
            if newer[0] != newer[1]:
                raise Refused(f'{target} has duplicate offsets after the copied history: {newer}')
            report[table] = {'copied_rows': copied, 'legacy': legacy[table], 'target_history': copy_stats,
                             'target_rows_after_history': newer[0]}
            print(f'{target}: copied {copied}; history matches {LEGACY} '
                  + ', '.join(f"p{p} rows={v['rows']} offsets={v['min_offset']}..{v['max_offset']} distinct={v['distinct_offsets']}"
                              for p, v in legacy[table].items()) + f'; newer rows {newer[0]}')
        if layout == 'none':
            alignment, problems = kafka_alignment(legacy)
            for row in alignment:
                print('Kafka ' + json.dumps(row))
            if problems:
                raise Refused('Kafka group offsets do not continue the copied history:\n  ' + '\n  '.join(problems)
                              + '\n' + remediation(alignment))
            record['kafka_alignment'] = alignment
        record.update(backfill=report, verified_at=now())
        save(record)
        print(f'Per-topic Bronze verified against {LEGACY}; {LEGACY} was not modified.')
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()


def needs_submit():
    record = load()
    layout, job_id = job_layout()
    if layout == 'per_topic':
        print(f'Per-topic job {job_id} is running; no submission.')
        return 0
    if layout == 'single':
        raise Refused('The single-table job is running; run the stop stage first')
    if not record.get('verified_at'):
        raise Refused('Backfill verification has not passed; run the backfill stage first')
    print('No cdc-bronze job is running; the per-topic job must be submitted.')
    return NEEDS_SUBMIT


def summary():
    record = load()
    deadline = time.monotonic() + 120
    while True:
        layout, job_id = job_layout()
        checkpoints = None
        if layout == 'per_topic':
            checkpoints = flink_state.requests.get(f'{flink_state.BASE}/jobs/{job_id}/checkpoints', timeout=10).json()
            if checkpoints['counts']['completed'] >= 1:
                break
        if time.monotonic() > deadline:
            raise Refused(f'Per-topic job not checkpointing within 120 s: layout={layout} job={job_id} '
                          f'checkpoints={(checkpoints or {}).get("counts")}')
        time.sleep(1)
    job = flink_state.requests.get(f'{flink_state.BASE}/jobs/{job_id}', timeout=10).json()
    completed = [c for c in checkpoints.get('history', []) if c.get('status') == 'COMPLETED']
    first = min(completed, key=lambda c: c['id']) if completed else checkpoints['latest']['completed']
    record.setdefault('new_job_id', job_id)
    record.setdefault('new_job_started_at', datetime.fromtimestamp(job['start-time'] / 1000, timezone.utc).isoformat())
    record.setdefault('first_checkpoint_at', datetime.fromtimestamp(first['latest_ack_timestamp'] / 1000, timezone.utc).isoformat())
    if record.get('stop_requested_at'):
        def seconds(a, b):
            return round((datetime.fromisoformat(record[b]) - datetime.fromisoformat(record[a])).total_seconds(), 1)
        record['ingestion_pause_seconds'] = seconds('stop_requested_at', 'first_checkpoint_at')
        record['stop_to_new_job_seconds'] = seconds('stop_requested_at', 'new_job_started_at')
    counts = {f'lake.bronze.{t}': spark(f'SELECT count(*) FROM lake.bronze.{t}')[0][0] for t in TABLES}
    if 'cdc_events' in bronze_tables():
        counts[LEGACY + ' (kept, no longer written)'] = spark(f'SELECT count(*) FROM {LEGACY}')[0][0]
    record.update(completed=True, completed_at=now(), row_counts=counts,
                  job={'id': job_id, 'state': job['state'], 'sinks': sorted(flink_state.sink_tables(job_id)),
                       'vertices': [v['name'] for v in job['vertices']], 'checkpoints': checkpoints['counts']})
    save(record)
    out = Path('artifacts/runs') / f"bronze-migration-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2, default=str))
    print(json.dumps({k: record.get(k) for k in ('legacy_job_id', 'legacy_savepoint', 'new_job_id', 'stop_requested_at',
                                                 'stopped_at', 'new_job_started_at', 'first_checkpoint_at',
                                                 'stop_to_new_job_seconds', 'ingestion_pause_seconds', 'row_counts')},
                     indent=2, default=str))
    print(f'PASS: per-topic Bronze is active. Record: {out}')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('stage', choices=['stop', 'backfill', 'needs-submit', 'summary'])
    stage = parser.parse_args().stage
    try:
        code = {'stop': stop, 'backfill': backfill, 'needs-submit': needs_submit, 'summary': summary}[stage]()
    except Refused as error:
        print(f'REFUSED ({stage}): {error}', file=sys.stderr)
        print(f'{LEGACY} is untouched. Fix the cause and rerun ./scripts/migrate-bronze-per-topic.sh.', file=sys.stderr)
        return 2
    return code or 0


if __name__ == '__main__':
    raise SystemExit(main())
