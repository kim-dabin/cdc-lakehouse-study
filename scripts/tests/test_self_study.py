"""Contract and failure tests without Docker, credentials or source mutations."""
import contextlib
import copy
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.self_study import (REQUIRED, StudyRun, check_physical, check_stage,
                               compose_rows, ensure_env, fresh_case, main)
from labs.session02.event_parser import parse_event

CASE = 's01-self-contract'
RID = CASE + '-r1'


def samples(stage):
    envelopes = [
        {'op': 'c', 'before': None, 'after': {'refund_id': RID, 'amount': 20000}},
        {'op': 'u', 'before': {'refund_id': RID, 'amount': 20000},
         'after': {'refund_id': RID, 'amount': 30000}},
        {'op': 'd', 'before': {'refund_id': RID, 'amount': 30000}, 'after': None},
    ][:stage]
    kafka, bronze = [], []
    for offset, value in enumerate(envelopes, 40):
        value['source'] = {'lsn': offset + 1000}
        payload = json.dumps(value)
        metadata = {'topic': 'cdc.public.refunds', 'partition_id': 0,
                    'kafka_offset': offset, 'kafka_ts_ms': 1760000000000}
        kafka.append({'metadata': metadata, 'key': json.dumps({'refund_id': RID}), 'value': payload})
        bronze.append({'topic': metadata['topic'], 'partition_id': 0, 'kafka_offset': offset,
                       'payload': payload, 'op': value['op']})
    if stage == 3:
        kafka.append({'metadata': {'topic': 'cdc.public.refunds', 'partition_id': 0,
                                  'kafka_offset': 43, 'kafka_ts_ms': 1760000000001},
                      'key': json.dumps({'refund_id': RID}), 'value': None})
    source = [] if stage == 3 else [{'refund_id': RID, 'payment_id': CASE + '-p1',
                                    'amount': 20000 if stage == 1 else 30000, 'status': 'success'}]
    return {'source': {'source_rows': source}, 'kafka': {'kafka_records': kafka},
            'bronze': {'bronze_records': bronze, 'record_count': len(bronze),
                       'history_ops': [row['op'] for row in bronze]},
            'parse': {'parsed_events': [parse_event(row['key'], row['value'], row['metadata']) for row in kafka]},
            'collect-check': {'passed': True, 'checks': [{'bronze_copies': 1} for _ in bronze]}}


def physical(count):
    # One file can contain all target rows and additional keys.
    return {'passed': True, 'sql_file_records_match': True, 'matching_record_count': count,
            'three_event_history_complete': count == 3,
            'physical_files': [{'byte_count': 1234, 'magic_start': 'PAR1', 'magic_end': 'PAR1',
                                'decoded_record_count': count + 9}]}


class FakeRun(StudyRun):
    def __init__(self, directory, environment='reuse', with_marts=False):
        super().__init__(directory, directory, CASE, environment, with_marts)
        self.calls, self.stage = [], 0
        self.services = [{'Service': name, 'State': 'running'} for name in REQUIRED]
        self.collision = False

    def command(self, name, argv, timeout=180, as_json=False):
        self.calls.append((name, argv))
        if name == 'docker-engine':
            return 'linux aarch64 8 17179869184\n'
        if name == 'containers-before':
            return '\n'.join(json.dumps(row) for row in self.services)
        if name == 'bootstrap':
            return 'baseline environment prepared'
        if name.startswith('preflight-'):
            return {'ready': True, 'job_id': 'same-job', 'checkpoint_counts': {'failed': 1, 'completed': 10},
                    'row_counts': {'lake.bronze.cdc_events': 2 if name.endswith('before') else 7}}
        if name in ('source-config', 'connector-config'):
            return {}
        if name == 'fresh-source-key':
            return {'orders': int(self.collision), 'payments': 0, 'refunds': 0}
        if name == 'fresh-bronze-key':
            return []
        if name.endswith('-write'):
            self.stage = {'payment': 0, 'refund': 1, 'correction': 2, 'delete': 3}[name[:-6]]
            return 'COMMIT'
        if name.endswith('-wait'):
            return {'bronze_visible': True, 'wait_observed_seconds': 0.1}
        if name == 'payment-source':
            return {'orders': 1, 'payments': 1, 'refunds': 0}
        if name == 'payment-bronze':
            return [['cdc.public.orders', 0, 1, json.dumps({'op': 'c', 'after': {'order_id': CASE + '-o1'}})],
                    ['cdc.public.payments', 0, 2, json.dumps({'op': 'c', 'after': {'amount': 100000}})]]
        if name.endswith('-refresh-marts'):
            return 'models and tests passed'
        if name.endswith('-verify-marts'):
            expected = int(argv[argv.index('--expected') + 1])
            return {'passed': True, 'gold_order': [[CASE + '-o1', '2026-10-19', 100000, 0, expected]]}
        if name.endswith('-parquet'):
            return physical(self.stage)
        action = name.split('-', 1)[1]
        mapping = {'source': 'source', 'kafka': 'kafka', 'bronze': 'bronze',
                   'parse': 'parse', 'collect': 'collect-check'}
        return copy.deepcopy(samples(self.stage)[mapping[action]])


class SelfStudyTests(unittest.TestCase):
    def check(self, data, action='delete'):
        return check_stage(data['source'], data['kafka'], data['bronze'], data['parse'],
                           data['collect-check'], action, CASE)

    def test_three_realistic_stages_and_raw_wire_tombstone(self):
        for stage, action, count in [(1, 'refund', 1), (2, 'correction', 2), (3, 'delete', 4)]:
            with self.subTest(action=action):
                result = self.check(samples(stage), action)
                self.assertEqual(result['kafka_records'], count)
                self.assertTrue(result['passed'])

    def test_empty_value_and_json_null_are_not_tombstones(self):
        for invalid_value in ('', 'null', '{"after":null}'):
            data = samples(3)
            data['kafka']['kafka_records'][-1]['value'] = invalid_value
            with self.subTest(value=invalid_value), self.assertRaises(RuntimeError):
                self.check(data)

    def test_key_payload_mismatch_is_rejected(self):
        data = samples(3)
        data['kafka']['kafka_records'][1]['key'] = '{"refund_id":"another-key"}'
        with self.assertRaisesRegex(RuntimeError, 'key'):
            self.check(data)

    def test_duplicate_bronze_location_is_rejected(self):
        data = samples(3)
        data['bronze']['bronze_records'].append(copy.deepcopy(data['bronze']['bronze_records'][0]))
        data['bronze']['record_count'] += 1
        with self.assertRaisesRegex(RuntimeError, 'Bronze count'):
            self.check(data)

    def test_equal_counts_do_not_hide_different_payload_or_identity(self):
        for field, value in [('payload', '{}'), ('kafka_offset', 999)]:
            data = samples(3)
            data['bronze']['bronze_records'][0][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, 'payload or Kafka identity'):
                self.check(data)

    def test_source_delete_and_one_copy_are_both_required(self):
        data = samples(3)
        data['source']['source_rows'] = [{'refund_id': RID, 'amount': 30000}]
        with self.assertRaisesRegex(RuntimeError, 'source row count'):
            self.check(data)
        data = samples(3)
        data['collect-check']['checks'][0]['bronze_copies'] = 2
        with self.assertRaisesRegex(RuntimeError, 'one Bronze copy'):
            self.check(data)

    def test_physical_proof_not_file_count_equals_target_count(self):
        self.assertEqual(check_physical(physical(3), 3), 1)
        data = physical(3)
        data['matching_record_count'] = 2
        with self.assertRaises(RuntimeError):
            check_physical(data, 3)
        data = physical(3)
        data['physical_files'][0]['magic_end'] = 'FAIL'
        with self.assertRaises(RuntimeError):
            check_physical(data, 3)

    def test_existing_env_and_symlink_are_never_overwritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / '.env.example').write_text('example-only\n')
            self.assertEqual(ensure_env(root), 'created_from_teaching_example')
            (root / '.env').write_text('existing-private-value\n')
            self.assertEqual(ensure_env(root), 'preserved')
            self.assertEqual((root / '.env').read_text(), 'existing-private-value\n')
            (root / '.env').unlink()
            target = root / 'absent'
            (root / '.env').symlink_to(target)
            self.assertEqual(ensure_env(root), 'preserved')
            self.assertFalse(target.exists())

    def test_compose_array_and_json_lines(self):
        rows = [{'Service': 'runner', 'State': 'running'}, {'Service': 'minio-init', 'State': 'exited'}]
        self.assertEqual(compose_rows(json.dumps(rows)), rows)
        self.assertEqual(compose_rows('\n'.join(map(json.dumps, rows))), rows)
        self.assertEqual(compose_rows(''), [])

    def test_session03_helper_does_not_shadow_stdlib_inspect(self):
        directory = ROOT / 'labs/session03'
        program = ('import sys; sys.path.insert(0, ' + repr(str(directory)) + '); '
                   'import inspect; assert callable(inspect.getmembers); '
                   'assert callable(inspect.signature)')
        subprocess.run([sys.executable, '-c', program], cwd=ROOT, check=True,
                       capture_output=True, text=True)
        self.assertFalse((directory / 'inspect.py').exists())

    def test_plan_requires_no_docker_and_creates_no_output(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            path = Path(tmp) / 'must-not-exist'
            self.assertEqual(main(['--plan', '--output-dir', str(path)]), 0)
            self.assertFalse(path.exists())
        self.assertRegex(fresh_case(), r'^s01-[a-z0-9-]{3,48}$')
        self.assertNotEqual(fresh_case(), fresh_case())

    def test_reuse_flow_has_four_explicit_writes_and_no_bootstrap_or_stop(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            run = FakeRun(Path(tmp))
            run.execute()
            names = [name for name, _ in run.calls]
            self.assertEqual([name for name in names if name.endswith('-write')],
                             ['payment-write', 'refund-write', 'correction-write', 'delete-write'])
            self.assertNotIn('bootstrap', names)
            self.assertFalse(any('stop' in arg or 'down' == arg for _, argv in run.calls for arg in argv))
            self.assertFalse(any('refresh-marts' in name for name in names))
            self.assertEqual([stage['bronze_records'] for stage in run.stages], [1, 2, 3])
            self.assertEqual([stage['physical_files_read'] for stage in run.stages], [1, 1, 1])
            self.assertEqual(run.result['status'], 'passed')

    def test_case_collision_fails_before_any_write(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            run = FakeRun(Path(tmp))
            run.collision = True
            with self.assertRaisesRegex(RuntimeError, 'already exists'):
                run.execute()
            self.assertFalse(any(name.endswith('-write') for name, _ in run.calls))

    def test_partial_start_refuses_bootstrap_and_active_airflow_refuses_isolated_run(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            run = FakeRun(Path(tmp), environment='start')
            with self.assertRaisesRegex(RuntimeError, 'Some project services are active'):
                run.execute()
            self.assertFalse(any(name == 'bootstrap' for name, _ in run.calls))
            run = FakeRun(Path(tmp))
            run.services.append({'Service': 'airflow', 'State': 'running'})
            with self.assertRaisesRegex(RuntimeError, 'Airflow is active'):
                run.execute()
            self.assertFalse(any(name.endswith('-write') for name, _ in run.calls))

    def test_marts_opt_in_checks_all_four_expected_values(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            run = FakeRun(Path(tmp), with_marts=True)
            run.execute()
            values = [int(argv[argv.index('--expected')+1]) for name, argv in run.calls if name.endswith('-verify-marts')]
            self.assertEqual(values, [100000, 80000, 70000, 100000])

    def test_command_preserves_each_occurrence_and_failure_exit(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()):
            run = StudyRun(Path(tmp), Path(tmp), CASE, 'reuse')
            first = run.command('same-action', [sys.executable, '-c', 'print("{\\"n\\":1}")'], as_json=True)
            second = run.command('same-action', [sys.executable, '-c', 'print("{\\"n\\":2}")'], as_json=True)
            self.assertEqual((first['n'], second['n']), (1, 2))
            self.assertEqual(json.loads((Path(tmp)/'01-same-action/result.json').read_text())['n'], 1)
            with self.assertRaisesRegex(RuntimeError, 'command failed'):
                run.command('fails', [sys.executable, '-c', 'raise SystemExit(7)'])
            self.assertEqual(run.steps[-1]['exit_code'], 7)
            self.assertEqual(len(run.steps), 3)


if __name__ == '__main__':
    unittest.main()
