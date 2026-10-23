"""Run one new common example locally and preserve evidence at every stage.

No reset, volume deletion, automatic shutdown, consumer group or offset commit.
The initial bootstrap prepares baseline marts; new-case mart refresh is opt-in.
Host requirements: Python 3.9+, Bash, Docker Engine and Docker Compose v2.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = {'source-db', 'catalog-db', 'kafka', 'connect', 'minio', 'jobmanager',
            'taskmanager', 'spark', 'runner'}
CASE_PATTERN = r's01-[a-z0-9-]{3,48}'
STAGES = [('refund', 'insert', 20000, 1, 1, 80000),
          ('correction', 'update', 30000, 2, 2, 70000),
          ('delete', 'delete', None, 4, 3, 100000)]
HISTORY = [('c', None, 20000), ('u', 20000, 30000), ('d', 30000, None)]


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def fresh_case():
    return 's01-self-' + datetime.now(timezone.utc).strftime('%m%d%H%M%S') + '-' + uuid.uuid4().hex[:8]


def ensure_env(root):
    """Exclusive creation; an existing .env (including a symlink) is untouched."""
    path = root / '.env'
    if path.exists() or path.is_symlink():
        return 'preserved'
    template = (root / '.env.example').read_bytes()
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return 'preserved'
    with os.fdopen(fd, 'wb') as stream:
        stream.write(template)
    return 'created_from_teaching_example'


def compose_rows(text):
    """Compose v2 releases emit either one JSON array or JSON lines."""
    text = text.strip()
    if not text:
        return []
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return [json.loads(line) for line in text.splitlines() if line.strip()]
    return value if isinstance(value, list) else [value]


def check_stage(source, kafka, bronze, parsed, collection, action, namespace):
    index = [item[0] for item in STAGES].index(action)
    _, _, amount, kafka_count, bronze_count, net = STAGES[index]
    rid = namespace + '-r1'
    rows = source['source_rows']
    require(len(rows) == (0 if action == 'delete' else 1), 'Unexpected current source row count')
    if rows:
        require(rows[0]['refund_id'] == rid and rows[0]['amount'] == amount and
                rows[0]['status'] == 'success', 'Unexpected current source refund')
    messages = kafka['kafka_records']
    require(len(messages) == kafka_count, 'Unexpected Kafka count (including tombstones)')
    from labs.session02.event_parser import parse_event
    events = []
    identities = []
    for row in messages:
        require(not row.get('key_wire_base64') and not row.get('value_wire_base64'),
                'Unexpected non-UTF8 wire bytes in this synthetic example')
        event = parse_event(row['key'], row['value'], row['metadata'])
        require(event.get('pk') == {'refund_id': rid}, 'Kafka key differs from the source PK')
        events.append(event)
        m = row['metadata']
        identities.append((m['topic'], m['partition_id'], m['kafka_offset']))
    require(len(set(identities)) == len(identities), 'Repeated Kafka identity')
    changes = [event for event in events if event['kind'] == 'change']
    expected = HISTORY[:bronze_count]
    require([(event['op'], (event['before'] or {}).get('amount'),
              (event['after'] or {}).get('amount')) for event in changes] == expected,
            'Kafka before/after history differs from c20k/u30k/d30k')
    tombstones = [event for event in events if event['kind'] == 'tombstone']
    require(len(tombstones) == (1 if action == 'delete' else 0), 'Unexpected wire-null tombstone count')
    require(all(event['kind'] in ('change', 'tombstone') for event in events), 'Invalid parsed live event')
    if tombstones:
        require(messages[-1]['value'] is None and events[-1]['kind'] == 'tombstone',
                'A JSON null or after:null is not a wire-null tombstone')
    records = bronze['bronze_records']
    require(bronze['record_count'] == len(records) == bronze_count, 'Unexpected Bronze count')
    require(bronze['history_ops'] == [row[0] for row in expected], 'Unexpected Bronze op history')
    actual = {(row['topic'], row['partition_id'], row['kafka_offset']): row['payload'] for row in records}
    expected_payloads = {(row['metadata']['topic'], row['metadata']['partition_id'],
                          row['metadata']['kafka_offset']): row['value']
                         for row in messages if row['value'] is not None}
    require(len(actual) == len(records) and actual == expected_payloads,
            'Bronze payload or Kafka identity differs from the live retained log')
    require(parsed['parsed_events'] == events, 'Independent parser output differs from the raw Kafka records')
    require(collection.get('passed') is True and len(collection['checks']) == bronze_count and
            all(row['bronze_copies'] == 1 for row in collection['checks']),
            'Collection check must find one Bronze copy per Kafka business event')
    return {'action': action, 'source_rows': len(rows), 'source_amount': amount,
            'kafka_records': len(messages), 'kafka_tombstones': len(tombstones),
            'bronze_records': len(records), 'history_ops': bronze['history_ops'],
            'expected_net_paid_after_refresh': net, 'passed': True}


def check_physical(data, expected_count):
    require(data.get('passed') is True and data.get('sql_file_records_match') is True and
            data.get('matching_record_count') == expected_count, 'Physical Parquet records do not match Bronze SQL')
    files = data.get('physical_files', [])
    require(bool(files), 'No physical Parquet file was read')
    for item in files:
        require(item['byte_count'] > 0 and item['magic_start'] == item['magic_end'] == 'PAR1',
                'Physical bytes are not the expected Parquet file')
    if expected_count == 3:
        require(data.get('three_event_history_complete') is True, 'Physical c/u/d history is incomplete')
    return len(files)


class StudyRun:
    def __init__(self, root, directory, namespace, environment, with_marts=False):
        self.root, self.directory, self.namespace = root, directory, namespace
        self.environment, self.with_marts = environment, with_marts
        self.steps, self.stages = [], []
        self.result = {'case_id': namespace, 'environment_mode': environment,
                       'with_marts': with_marts, 'status': 'running', 'stages': self.stages,
                       'started_at': datetime.now(timezone.utc).isoformat(),
                       'scope': 'One synthetic key. Wait timing is command start to first Bronze observation, not source-commit latency. No exactly-once or recovery guarantee.'}

    def save(self):
        (self.directory / 'steps.json').write_text(json.dumps(self.steps, ensure_ascii=False, indent=2) + '\n')
        (self.directory / 'report.json').write_text(json.dumps(self.result, ensure_ascii=False, indent=2) + '\n')
        lines = ['# 공통 예제 독립 실행 결과', '', f'- CASE_ID: `{self.namespace}`',
                 f'- 결과: **{self.result["status"]}**', f'- 실행 환경: `{self.environment}`',
                 '- 기존 .env와 볼륨은 덮어쓰거나 삭제하지 않습니다.',
                 '- 최초 bootstrap은 기준 Silver·Gold를 준비합니다. 새 사건의 마트 갱신은 --with-marts 선택입니다.',
                 '', '| 변경 | 원천 환불 행 | 원천 금액 | Kafka(삭제표식 포함) | Bronze 이력 | 마트 갱신 후 순결제액 |',
                 '|---|---:|---:|---:|---:|---|']
        for stage in self.stages:
            gold = str(stage.get('gold_net_paid')) if 'gold_net_paid' in stage else '실행하지 않음'
            lines.append(f'| {stage["action"]} | {stage["source_rows"]} | {stage["source_amount"]} | {stage["kafka_records"]} | {stage["bronze_records"]} | {gold} |')
        lines += ['', 'Kafka의 DELETE 이벤트는 before가 있는 JSON이고 tombstone은 같은 키의 wire value 전체가 null인 별도 레코드입니다. Bronze는 tombstone을 제외하고 c/u/d 이력을 보존합니다.',
                  '', '파일 수는 이벤트 수와 같지 않을 수 있습니다. 각 단계 parquet.json은 실제 파일 바이트를 읽고 디코드한 행을 SQL 결과와 대조합니다.',
                  '', 'wait_observed_seconds는 wait 명령 시작부터 첫 Bronze 관측까지입니다. 원천 커밋부터의 지연이나 고정된 처리시간을 뜻하지 않습니다.',
                  '', '환경은 자동 종료하지 않습니다. 필요한 경우 별도로 ./scripts/stop.sh를 실행해 savepoint를 남기고 중지합니다. 실패해도 자동 초기화·DML 재시도·볼륨 삭제를 하지 않습니다.',
                  '', '단계별 stdout/stderr와 원문 JSON은 이 보고서와 같은 디렉터리에 있습니다. 실패한 경우 steps.json에서 마지막 단계와 로그를 먼저 확인하고 원인을 해결한 뒤 새 CASE_ID로 다시 실행합니다.']
        if self.result.get('error'):
            lines += ['', '실패 이유: ' + self.result['error']]
        (self.directory / 'report.md').write_text('\n'.join(lines) + '\n')

    def command(self, name, argv, timeout=180, as_json=False):
        number = len(self.steps) + 1
        directory = self.directory / f'{number:02d}-{name}'
        directory.mkdir()
        record = {'step': number, 'name': name, 'argv': argv,
                  'stdout': str(directory.relative_to(self.directory) / 'stdout.txt'),
                  'stderr': str(directory.relative_to(self.directory) / 'stderr.txt')}
        self.steps.append(record)
        self.save()
        print(f'[{number:02d}] {name}', flush=True)
        started = time.monotonic()
        try:
            with (directory / 'stdout.txt').open('w') as stdout, (directory / 'stderr.txt').open('w') as stderr:
                process = subprocess.Popen(argv, cwd=self.root, stdout=stdout, stderr=stderr)
                try:
                    while process.poll() is None:
                        try:
                            process.wait(timeout=min(30, max(0.01, timeout - (time.monotonic() - started))))
                        except subprocess.TimeoutExpired:
                            require(time.monotonic() - started < timeout, f'{name}: timeout; see saved logs')
                            print(f'     {int(time.monotonic()-started)}초 경과: 실행 중, 로그를 보관하고 있습니다.', flush=True)
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=5)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait()
                record['exit_code'] = process.returncode
            require(process.returncode == 0, f'{name}: command failed; see {record["stderr"]}. No automatic retry or cleanup.')
            text = (directory / 'stdout.txt').read_text()
            data = json.loads(text) if as_json else text
            if as_json:
                (directory / 'result.json').write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')
            return data
        except BaseException as error:
            record['error_type'] = type(error).__name__
            raise
        finally:
            record['seconds'] = round(time.monotonic() - started, 3)
            self.save()

    def lab(self, name, action, *options):
        return self.command(name, ['./labs/session02/lab.sh', self.namespace, action, '--evidence', *options],
                            timeout=360, as_json=True)

    def sql(self, name, sql):
        return self.command(name, ['./scripts/lab.sh', 'exec', '-T', 'runner', 'python',
                                  'scripts/pipeline.py', '--sql', sql], as_json=True)

    def source_case(self, name):
        n = self.namespace
        sql = f"""SELECT json_build_object(
          'orders',(SELECT count(*) FROM orders WHERE order_id='{n}-o1'),
          'payments',(SELECT count(*) FROM payments WHERE payment_id='{n}-p1'),
          'refunds',(SELECT count(*) FROM refunds WHERE refund_id='{n}-r1'))"""
        return self.command(name, ['./scripts/lab.sh', 'exec', '-T', 'source-db', 'psql', '-X', '-q', '-t', '-A',
                                   '-v', 'ON_ERROR_STOP=1', '-U', 'source', '-d', 'app', '-c', sql], as_json=True)

    def bronze_case(self, name):
        n = self.namespace
        predicates = [f"get_json_object(payload,'$.{part}.{pk}')='{n}-{suffix}'"
                      for part in ('before', 'after')
                      for pk, suffix in [('order_id', 'o1'), ('payment_id', 'p1'), ('refund_id', 'r1')]]
        return self.sql(name, 'SELECT topic,partition_id,kafka_offset,payload FROM lake.bronze.cdc_events WHERE ' +
                        ' OR '.join(predicates) + ' ORDER BY topic,partition_id,kafka_offset')

    def marts(self, action, expected):
        self.command(action + '-refresh-marts', ['./labs/session01/lab.sh', self.namespace, 'refresh'], timeout=600)
        data = self.command(action + '-verify-marts', ['./labs/session01/lab.sh', self.namespace, 'check',
                            '--expected', str(expected), '--evidence'], timeout=300, as_json=True)
        require(data.get('passed') is True and data['gold_order'][0][4] == expected,
                'Refreshed Gold net_paid differs from the expected successful-payment minus refund amount')
        return expected

    def execute(self):
        # Inspect state first. Neither reuse nor failed startup silently rewrites configuration.
        info_script = 'set -eu; if [ -f .runtime/host.env ]; then . .runtime/host.env; fi; exec docker info --format "{{.OSType}} {{.Architecture}} {{.NCPU}} {{.MemTotal}}"'
        info = self.command('docker-engine', ['bash', '-c', info_script]).strip().split()
        require(info and info[0] == 'linux', 'Docker must run Linux containers')
        self.result['docker_engine'] = {'os': info[0], 'architecture': info[1], 'cpus': info[2], 'memory_bytes': info[3]}
        if info[1] not in ('aarch64', 'arm64'):
            print('주의: 기준 실행 검증은 Linux ARM64입니다. 이 CPU의 이미지 호환성과 성능은 별도 확인이 필요합니다.', flush=True)
        rows = compose_rows(self.command('containers-before', ['./scripts/lab.sh', 'ps', '--all', '--format', 'json']))
        active = {row['Service'] for row in rows if row.get('State') not in ('exited', 'dead', 'created')}
        require('airflow' not in active, 'Airflow is active: inspect its schedule separately before this isolated common-example run; nothing was stopped automatically')
        if self.environment == 'reuse':
            running = {row['Service'] for row in rows if row.get('State') == 'running'}
            require(REQUIRED <= running, 'Required services are not running. For a stopped prepared environment use --environment start; first install uses --environment first')
            self.result['env_file'] = 'preserved; not read'
        else:
            require(not active, 'Some project services are active. Use --environment reuse or inspect the partial startup; automatic restart is refused')
            self.result['env_file'] = ensure_env(self.root)
            option = '--build' if self.environment == 'first' else '--no-build'
            self.command('bootstrap', ['./scripts/bootstrap.sh', option], timeout=2400)
        baseline = self.command('preflight-before', ['./scripts/lab.sh', 'exec', '-T', 'runner', 'python',
                                                     'labs/session01/preflight.py'], timeout=300, as_json=True)
        require(baseline.get('ready') is True, 'Initial data-query readiness did not pass')
        self.result['preflight_before'] = baseline
        self.command('source-config', ['./labs/session02/lab.sh', 'source-config', '--evidence'], as_json=True)
        self.command('connector-config', ['./labs/session02/lab.sh', 'connector-sanitized', '--evidence'], as_json=True)
        require(all(value == 0 for value in self.source_case('fresh-source-key').values()), 'CASE_ID already exists in source tables. Choose a new key; nothing was reset')
        require(not self.bronze_case('fresh-bronze-key'), 'CASE_ID already exists in Bronze history. Choose a new key; nothing was reset')
        self.command('payment-write', ['./labs/session02/lab.sh', self.namespace, 'payment'])
        self.lab('payment-wait', 'wait', '--event', 'payment')
        payment_source = self.source_case('payment-source')
        payment_bronze = self.bronze_case('payment-bronze')
        require(payment_source == {'orders': 1, 'payments': 1, 'refunds': 0}, 'Payment transaction did not create one order and one payment')
        require(len(payment_bronze) == 2 and {row[0] for row in payment_bronze} == {'cdc.public.orders', 'cdc.public.payments'}, 'Expected exactly two Bronze events for the new order/payment')
        payment = next(json.loads(row[3]) for row in payment_bronze if row[0] == 'cdc.public.payments')
        require(payment.get('op') == 'c' and payment['after']['amount'] == 100000, 'Unexpected payment Bronze payload')
        self.result['payment'] = {'source': payment_source, 'bronze_records': 2,
                                  'gold_net_paid': self.marts('payment', 100000) if self.with_marts else None}
        for action, stage, amount, kafka_count, bronze_count, net in STAGES:
            self.command(action + '-write', ['./labs/session02/lab.sh', self.namespace, action])
            wait = self.lab(action + '-wait', 'wait', '--event', action)
            source = self.lab(action + '-source', 'source-rows')
            kafka = self.lab(action + '-kafka', 'kafka')
            bronze = self.lab(action + '-bronze', 'bronze')
            parsed = self.lab(action + '-parse', 'parse')
            collection = self.lab(action + '-collect', 'collect-check', '--stage', stage)
            result = check_stage(source, kafka, bronze, parsed, collection, action, self.namespace)
            physical = self.lab(action + '-parquet', 'parquet')
            result['physical_files_read'] = check_physical(physical, bronze_count)
            result['wait_observed_seconds'] = wait['wait_observed_seconds']
            if self.with_marts:
                result['gold_net_paid'] = self.marts(action, net)
            self.stages.append(result)
            self.save()
            print(f'     확인: 원천 {result["source_rows"]}행 / Kafka {kafka_count}건 / Bronze {bronze_count}건 / 실제 Parquet 일치', flush=True)
        final = self.command('preflight-after', ['./scripts/lab.sh', 'exec', '-T', 'runner', 'python',
                                                 'labs/session01/preflight.py'], timeout=300, as_json=True)
        require(final.get('ready') is True and final['job_id'] == baseline['job_id'], 'Ingestion job changed or lost readiness during the run')
        require(final['checkpoint_counts']['failed'] == baseline['checkpoint_counts']['failed'], 'Checkpoint failure count increased during the run; inspect saved evidence')
        self.result['preflight_after'] = final
        self.result['status'] = 'passed'


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--environment', choices=('first', 'start', 'reuse'), default='reuse',
                        help='first: build stopped environment; start: reuse local images; reuse: no bootstrap (default)')
    result.add_argument('--with-marts', action='store_true', help='Also refresh Silver/dbt and check Gold after every change')
    result.add_argument('--case-id', help='Optional unused s01- key; automatic fresh key is recommended')
    result.add_argument('--output-dir', type=Path, help='New directory only; existing evidence is never overwritten')
    result.add_argument('--plan', action='store_true', help='Print the plan without Docker, files or DML')
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    namespace = args.case_id or fresh_case()
    if not re.fullmatch(CASE_PATTERN, namespace):
        parser().error('CASE_ID must match s01- followed by 3-48 lowercase letters, digits or hyphens')
    print('실행 위치: 현재 컴퓨터의 Docker 환경 (SSH 또는 강사 서버를 사용하지 않습니다).', flush=True)
    print(f'CASE_ID={namespace}\n환경 모드={args.environment}; 새 사건의 마트 갱신={args.with_marts}', flush=True)
    if args.plan:
        print('Docker Linux ARM64 기준 검증. Docker Compose v2와 Python 3.9+가 필요하며 첫 build에는 네트워크와 충분한 VM 메모리/디스크가 필요합니다.')
        print('환경 확인 → (first/start만 bootstrap) → preflight → 새 키 충돌 확인 → payment → refund → correction → delete → 최종 preflight')
        print('각 환불 변경 뒤 wait → source/Kafka/Bronze/parser/collect-check → 실제 Parquet 바이트 검증. --with-marts 선택 시 Silver/dbt/Gold도 검증.')
        print('예상: 원천 환불 1→1→0, Kafka 1→2→4, Bronze 1→2→3. 마트 선택 시 100000→80000→70000→100000.')
        print('기존 .env와 볼륨은 보존하며 reset/down/stop/DML 자동 재시도를 하지 않습니다. --plan은 파일도 생성하지 않습니다.')
        return 0
    directory = (args.output_dir or ROOT / 'artifacts/runs/self-study' / namespace).resolve()
    try:
        directory.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        print('이미 존재하는 출력 디렉터리는 재사용하지 않습니다. 새 CASE_ID 또는 새 --output-dir를 사용하세요.', file=sys.stderr)
        return 2
    run = StudyRun(ROOT, directory, namespace, args.environment, args.with_marts)
    started = time.monotonic()
    try:
        run.execute()
    except (Exception, KeyboardInterrupt) as error:
        run.result.update(status='interrupted' if isinstance(error, KeyboardInterrupt) else 'failed', error=str(error))
        print('중단했습니다. 기존 데이터와 환경을 유지합니다. 보고서의 마지막 단계부터 확인하세요.', file=sys.stderr)
    finally:
        run.result.update(finished_at=datetime.now(timezone.utc).isoformat(), seconds=round(time.monotonic()-started, 3))
        run.save()
    print(f'결과: {run.result["status"]}\n보고서: {directory / "report.md"}', flush=True)
    return 0 if run.result['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.path.insert(0, str(ROOT))
    raise SystemExit(main())
