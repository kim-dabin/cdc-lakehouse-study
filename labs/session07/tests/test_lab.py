"""Spark 없이 SQL 순서·격리 범위·기대 판정을 확인해요."""
import contextlib
from datetime import datetime, timedelta
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
from unittest.mock import patch

if importlib.util.find_spec("pyhive") is None:
    with patch.dict(sys.modules, {"pyhive": types.SimpleNamespace(hive=None)}):
        from labs.session07 import lab as lab_module
        from labs.session07.lab import BASE_ROWS, BASE_SUM, Lesson, STEPS, expected, main, orphan_counts, metadata_version
else:
    from labs.session07 import lab as lab_module
    from labs.session07.lab import BASE_ROWS, BASE_SUM, Lesson, STEPS, expected, main, orphan_counts, metadata_version

ROOT = Path(__file__).resolve().parents[3]


class FakeCursor:
    """정해진 응답으로 SQL 계약을 확인해요. Spark SQL 실행기는 아니에요."""
    def __init__(self):
        self.queries = []
        self.rows = []
        self.description = []
        self.existing = []
        self.data = {t: [] for t in ("plain", "tuned", "inc")}
        self.history = {t: [] for t in self.data}
        self.props = {t: {} for t in self.data}
        self.metadata_count = {t: 1 for t in self.data}
        self.metadata_versions = {t: 0 for t in self.data}
        self.metadata_file_override = None
        self.orphans = [("s3://lab/old.metadata.json",), ("s3://lab/old.avro",),
                        ("s3://lab/old.parquet",), ("s3://lab/version-hint.text",)]
        self.fail_orphans = False
        self.safe_fails = False
        self.unsafe_succeeds = False
        self.lazy_failure = False
        self.view_start_missing = False
        self.clock = datetime(2026, 1, 2)

    def commit(self, table):
        # ID는 순서대로 커지지 않아요. SQL은 committed_at이나 refs를 써야 해요.
        self.clock += timedelta(seconds=1)
        self.history[table].append((10000 - len(self.history[table]) * 37, self.clock))
        self.metadata_count[table] += 1
        self.metadata_versions[table] += 1
        if table == "tuned":
            self.metadata_count[table] = min(11, self.metadata_count[table])

    def output(self, columns, rows):
        self.description = [(column,) for column in columns]
        self.rows = rows

    def execute(self, sql):
        sql = sql.strip()
        self.queries.append(sql)
        self.rows, self.description = [], []
        match = re.search(r"(?:lake\.)?lesson07\.s07_[a-z0-9_]+_(plain|tuned|inc)\b", sql)
        table = match[1] if match else None
        if sql.startswith("SHOW TABLES"):
            self.rows = self.existing
        elif sql == "SELECT current_timestamp()":
            self.rows = [(self.clock + timedelta(minutes=1),)]
        elif sql.startswith("SHOW TBLPROPERTIES"):
            self.rows = list(self.props[table].items())
        elif sql.startswith("ALTER TABLE"):
            self.props[table].update(re.findall(r"'([^']+)'='([^']+)'", sql))
            self.metadata_count[table] += 1
            self.metadata_versions[table] += 1
            if table == "tuned":
                self.metadata_count[table] = min(11, self.metadata_count[table])
        elif sql.startswith("INSERT INTO"):
            number = int(re.search(r"'e(\d+)'", sql)[1])
            self.data[table].append(number * 100)
            self.commit(table)
        elif sql.startswith("CALL lake.system.rewrite_data_files"):
            self.commit(table)
            self.output(("rewritten_data_files_count", "added_data_files_count", "rewritten_bytes_count",
                         "failed_data_files_count", "removed_delete_files_count"), [(3, 1, 1200, 0, 0)])
        elif sql.startswith("CALL lake.system.expire_snapshots"):
            cutoff = datetime.fromisoformat(re.search(r"TIMESTAMP '([^']+)'", sql)[1])
            retained = self.history[table][-1:]
            self.history[table] = [r for r in self.history[table] if r[1] >= cutoff or r in retained]
            self.output(("deleted_data_files_count", "deleted_position_delete_files_count",
                         "deleted_equality_delete_files_count", "deleted_manifest_files_count",
                         "deleted_manifest_lists_count", "deleted_statistics_files_count"), [(2, 0, 0, 2, 2, 0)])
        elif sql.startswith("CALL lake.system.remove_orphan_files"):
            if self.fail_orphans:
                raise RuntimeError("고아 파일 조회를 일부러 실패시켰어요.")
            self.output(("orphan_file_location",), list(self.orphans))
        elif sql.startswith("CALL lake.system.rewrite_manifests"):
            self.output(("rewritten_manifests_count", "added_manifests_count"), [(2, 1)])
        elif sql.startswith("CALL lake.system.create_changelog_view"):
            start = int(re.search(r"'start-snapshot-id','(\d+)'", sql)[1])
            self.view_start_missing = start not in {r[0] for r in self.history["inc"]}
            if self.safe_fails or (self.view_start_missing and not self.unsafe_succeeds and not self.lazy_failure):
                raise RuntimeError("시작 snapshot이 없어요. " + "가" * 400)
            self.output(("changelog_view",), [("가짜_뷰",)])
        elif sql.startswith("SELECT count(*), coalesce(sum(amount),0)"):
            self.rows = [(len(self.data[table]), sum(self.data[table]))]
        elif sql.startswith("SELECT count(*), coalesce(sum(file_size_in_bytes),0)"):
            self.rows = [(len(self.data[table]), len(self.data[table]) * 400)]
        elif sql.startswith("SELECT count(*) FROM s07_"):
            if self.view_start_missing and self.lazy_failure:
                raise RuntimeError("조회 시점에 시작 snapshot이 없어요.")
            self.rows = [(4,)]
        elif sql.startswith("SELECT count(DISTINCT "):
            self.rows = [(len(self.data[table]),)]
        elif sql.startswith("SELECT count(*)"):
            suffix = sql.rsplit(".", 1)[1]
            value = (len(self.history[table]) if suffix == "snapshots" else
                     self.metadata_count[table] if suffix == "metadata_log_entries" else 2)
            self.rows = [(value,)]
        elif sql.startswith("SELECT file FROM"):
            file = (self.metadata_file_override if self.metadata_file_override is not None else
                    f"s3://lab/metadata/{self.metadata_versions[table]:05d}-uuid.metadata.json")
            self.rows = [(file,)]
        elif sql.startswith("SELECT snapshot_id, committed_at"):
            self.rows = list(self.history[table])
        elif sql.startswith("SELECT snapshot_id"):
            rows = self.history[table][-1:] if ".refs" in sql else self.history[table]
            self.rows = [(r[0],) for r in rows]
        elif not sql.startswith(("CREATE NAMESPACE", "CREATE TABLE", "DROP TABLE", "SET spark.sql.session.timeZone=")):
            raise AssertionError("가짜 cursor가 모르는 SQL이에요: " + sql)

    def fetchall(self):
        return self.rows


class LabTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        quiet = contextlib.redirect_stdout(self.output)
        quiet.__enter__()
        self.addCleanup(quiet.__exit__, None, None, None)
        self.cur = FakeCursor()
        self.lesson = Lesson(self.cur, "s07-test")

    def grow(self, commits=3):
        self.lesson.grow(commits)
        self.cur.queries.clear()

    def test_run_id_validation_before_sql_and_prefix_rules(self):
        for run_id in ("s07-a", "s07-user_01", "s07-" + "a" * 36):
            Lesson(self.cur, run_id)
        for run_id in ("", "s07-", "s06-a", "S07-a", "s07-A", "s07-a.b", "s07-a;DROP",
                       "s07-a/b", "s07-a\n", "s07-" + "a" * 37):
            with self.subTest(run_id=run_id), self.assertRaises(ValueError):
                Lesson(self.cur, run_id)
        self.assertEqual(self.cur.queries, [])
        self.assertEqual(Lesson(None, "s07-a-b").tables, Lesson(None, "s07-a_b").tables)
        self.assertEqual(self.lesson.tables, {t: f"lake.lesson07.s07_test_{t}" for t in ("plain", "tuned", "inc")})

    def test_prepare_schema_properties_and_automatic_measure(self):
        self.lesson.prepare()
        self.assertEqual(self.cur.queries[:2], ["CREATE NAMESPACE IF NOT EXISTS lake.lesson07",
                                               "SHOW TABLES IN lake.lesson07"])
        creates = [q for q in self.cur.queries if q.startswith("CREATE TABLE")]
        self.assertEqual(len(creates), 3)
        for sql in creates:
            self.assertIn("event_id STRING, order_id STRING, amount BIGINT, event_ts TIMESTAMP", sql)
            self.assertIn("USING iceberg", sql)
        self.assertNotIn("TBLPROPERTIES", creates[0])
        self.assertIn("'write.metadata.delete-after-commit.enabled'='true'", creates[1])
        self.assertIn("'write.metadata.previous-versions-max'='10'", creates[1])
        self.assertNotIn("TBLPROPERTIES", creates[2])
        self.assertEqual(self.lesson.measurements[-1]["after"], "prepare")

    def test_prepare_collision_refuses_any_run_table_before_ddl(self):
        for suffix in ("plain", "tuned", "inc"):
            self.cur.existing = [("lesson07", f"s07_test_{suffix}", False)]
            self.cur.queries.clear()
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                self.lesson.prepare()
            self.assertFalse(any(q.startswith("CREATE TABLE") for q in self.cur.queries))

    def test_clean_only_three_exact_tables_without_discovery(self):
        self.cur.existing = [("lesson07", "s07_test_plain_backup", False),
                             ("lesson07", "s07_other_plain", False)]
        self.lesson.clean()
        self.assertEqual(self.cur.queries, [f"DROP TABLE IF EXISTS lake.lesson07.s07_test_{t} PURGE"
                                            for t in ("plain", "tuned", "inc")])
        self.assertIn("3개만", self.lesson.records[-1]["message"])

    def test_grow_deterministic_inserts_and_persisted_baseline(self):
        self.lesson.grow(4)
        inserts = [q for q in self.cur.queries if q.startswith("INSERT INTO")]
        self.assertEqual(len(inserts), 8)
        for number in range(1, 5):
            for offset, table in enumerate(("plain", "tuned")):
                self.assertEqual(inserts[(number - 1) * 2 + offset],
                                 f"INSERT INTO lake.lesson07.s07_test_{table} VALUES "
                                 f"('e{number:04d}', 'o{number:04d}', {number * 100}, "
                                 f"TIMESTAMP '2026-01-01 00:00:00' + INTERVAL {number} SECONDS)")
        self.assertEqual(self.lesson.baseline("plain"), {"rows": 4, "checksum": 1000})
        self.assertEqual(self.lesson.baseline("tuned"), expected(4))
        self.assertEqual(self.lesson.measurements[-1]["after"], "grow")
        self.assertFalse(self.cur.data["inc"])

    def test_grow_defaults_to_120_commits_and_tuned_history_caps_at_11(self):
        self.lesson.grow()
        self.assertEqual(len([q for q in self.cur.queries if q.startswith("INSERT INTO")]), 240)
        self.assertEqual(expected(120), {"rows": 120, "checksum": 726000})
        result = self.lesson.measurements[-1]["tables"]
        self.assertEqual(result["tuned"]["metadata_log"], 11)
        self.assertGreater(result["plain"]["metadata_log"], 100)

    def test_grow_rejects_invalid_or_repeated_input_before_insert(self):
        for commits in (0, -1, 1.5, True):
            with self.subTest(commits=commits), self.assertRaises(ValueError):
                self.lesson.grow(commits)
        self.assertFalse(self.cur.queries)
        self.cur.data["tuned"] = [100]
        with self.assertRaises(ValueError):
            self.lesson.grow(3)
        self.assertFalse(any(q.startswith("INSERT") for q in self.cur.queries))
        self.cur.data["tuned"] = []
        self.cur.props["tuned"] = {BASE_ROWS: "0", BASE_SUM: "0"}
        with self.assertRaises(ValueError):
            self.lesson.grow(3)

    def test_compact_arguments_result_columns_and_measure(self):
        self.grow()
        self.lesson.compact()
        calls = [q for q in self.cur.queries if q.startswith("CALL lake.system.rewrite_data_files")]
        self.assertEqual(calls, [f"CALL lake.system.rewrite_data_files(table => 'lesson07.s07_test_{t}', "
                                 "options => map('min-input-files','2'))" for t in ("plain", "tuned")])
        result = next(r for r in self.lesson.records if r["step"] == "compact")
        self.assertEqual(result["results"][0]["rewritten_data_files_count"], 3)
        self.assertIn("failed_data_files_count", result["columns"])
        self.assertEqual(self.lesson.measurements[-1]["after"], "compact")

    def test_expire_cutoff_retain_last_and_deleted_columns(self):
        self.grow()
        cutoff = self.cur.clock + timedelta(minutes=1)
        self.lesson.expire()
        calls = [q for q in self.cur.queries if q.startswith("CALL lake.system.expire_snapshots")]
        self.assertEqual(len(calls), 2)
        for sql in calls:
            self.assertIn(f"older_than => TIMESTAMP '{cutoff}', retain_last => 1", sql)
        record = next(r for r in self.lesson.records if r["step"] == "expire")
        self.assertEqual(len(record["columns"]), 6)
        self.assertTrue(all(c.startswith("deleted_") and c.endswith("_count") for c in record["columns"]))
        self.assertEqual(self.lesson.measurements[-1]["tables"]["plain"]["snapshots"], 1)

    def test_orphan_list_precedes_deletion_and_reuses_cutoff(self):
        self.grow()
        self.lesson.orphans()
        calls = [q for q in self.cur.queries if q.startswith("CALL lake.system.remove_orphan_files")]
        # 자동 측정은 orphan 프로시저를 호출하지 않아요.
        self.assertEqual(len(calls), 4)
        for call in calls:
            self.assertIn(", prefix_listing => true", call)
        now = self.cur.clock + timedelta(minutes=1)
        for index, table in ((0, "plain"), (2, "tuned")):
            self.assertIn(f"table => 'lesson07.s07_test_{table}'", calls[index])
            cutoff = datetime.fromisoformat(re.search(r"TIMESTAMP '([^']+)'", calls[index])[1])
            self.assertEqual(now - cutoff, timedelta(hours=24, minutes=5))
            self.assertLess(cutoff, now - timedelta(hours=24))
            self.assertEqual(calls[index].replace(", dry_run => true", ""), calls[index + 1])
        records = [r for r in self.lesson.records if r["step"] == "orphans"]
        self.assertEqual([r["dry_run"] for r in records], [True, False, True, False])
        self.assertEqual(records[0]["orphan_file_location"], [r[0] for r in self.cur.orphans])
        self.assertEqual(records[0]["counts"], {"metadata.json": 1, "avro": 1, "parquet": 1, "other": 1})
        self.assertEqual(len({r["older_than"] for r in records}), 1)
        for record in records:
            self.assertIn("대개 0개", record["message"])
            self.assertIn("아직 커밋하지 않은 파일", record["message"])

    def test_orphan_classification_uses_suffix_not_directory_name(self):
        rows = [("s3://lab/a.metadata.json",), ("s3://lab/b.metadata.json",),
                ("s3://lab/A.AVRO",), ("s3://lab/a.parquet",),
                ("s3://lab/parquet/version-hint.text",), ("s3://lab/a.metadata.json.crc",)]
        self.assertEqual(orphan_counts(rows), {"metadata.json": 2, "avro": 1, "parquet": 1, "other": 2})
        self.assertEqual(sum(orphan_counts([]).values()), 0)

    def test_no_static_testing_config_in_generated_sql(self):
        self.lesson.all(commits=3)
        self.assertFalse(any("spark.testing" in sql.lower() for sql in self.cur.queries))

    def test_orphan_failure_propagates_without_setting_sql(self):
        self.cur.fail_orphans = True
        with self.assertRaises(RuntimeError):
            self.lesson.orphans()
        self.assertFalse(any(q.startswith(("SET ", "RESET ")) for q in self.cur.queries))
        self.assertFalse(any(q.startswith("CALL") and "dry_run => true" not in q
                             for q in self.cur.queries))

    def test_fresh_orphans_records_zero_candidates_and_deletions(self):
        self.cur.orphans = []
        self.lesson.orphans()
        records = [r for r in self.lesson.records if r["step"] == "orphans"]
        self.assertEqual(len(records), 4)
        for record in records:
            self.assertEqual(record["orphan_file_location"], [])
            self.assertEqual(sum(record["counts"].values()), 0)

    def test_manifests_procedure_records_documented_columns(self):
        self.grow()
        self.lesson.manifests()
        calls = [q for q in self.cur.queries if q.startswith("CALL lake.system.rewrite_manifests")]
        self.assertEqual(calls, [f"CALL lake.system.rewrite_manifests(table => 'lesson07.s07_test_{t}')"
                                for t in ("plain", "tuned")])
        record = next(r for r in self.lesson.records if r["step"] == "manifests")
        self.assertEqual(record["results"], [{"rewritten_manifests_count": 2, "added_manifests_count": 1}])

    def test_measure_counts_all_tables_and_estimates_written_and_untracked(self):
        self.grow(120)
        self.cur.queries.clear()
        results = self.lesson.measure()
        for table in ("plain", "tuned", "inc"):
            value = results[table]
            for key in ("snapshots", "metadata_log", "data_files", "data_bytes", "all_data_files_distinct",
                        "manifests", "all_manifests_distinct", "rows", "checksum"):
                self.assertIn(key, value)
            for suffix, column in (("all_data_files", "file_path"), ("all_manifests", "path")):
                self.assertIn(
                    f"SELECT count(DISTINCT {column}) FROM {self.lesson.tables[table]}.{suffix}",
                    self.cur.queries)
                self.assertNotIn(suffix, value)
                self.assertEqual(value[f"{suffix}_distinct"], 0 if table == "inc" else 120)
            self.assertEqual(value["metadata_version"], 0 if table == "inc" else 121)
            self.assertEqual(value["metadata_json_written_est"], value["metadata_version"] + 1)
            self.assertEqual(value["metadata_untracked_est"],
                             value["metadata_json_written_est"] - value["metadata_log"])
            self.assertNotIn("orphan_candidates", value)
            self.assertIn("plain은 저장소에 남고", value["message"])
            self.assertIn("delete-after-commit", value["message"])
        self.assertEqual(results["tuned"]["metadata_untracked_est"], 111)
        self.assertTrue(results["plain"]["match"])
        self.assertIsNone(results["inc"]["match"])
        self.assertFalse(any(q.startswith(("CALL", "SET", "RESET")) for q in self.cur.queries))
        latest_queries = [q for q in self.cur.queries if q.startswith("SELECT file FROM")]
        self.assertEqual(len(latest_queries), 3)
        self.assertTrue(all(q.endswith("ORDER BY timestamp DESC LIMIT 1") for q in latest_queries))

    def test_metadata_version_parses_numbered_filename_only(self):
        for file, wanted in (
            ("s3://lab/metadata/00121-uuid.metadata.json", 121),
            ("00000-uuid.metadata.json", 0),
            ("s3://lab/metadata/v121.metadata.json", None),
            ("s3://lab/metadata/uuid.metadata.json", None),
            ("s3://lab/metadata/00121-.metadata.json", None),
            ("s3://lab/metadata/00121-uuid.metadata.json.crc", None),
            (None, None),
        ):
            with self.subTest(file=file):
                self.assertEqual(metadata_version(file), wanted)

    def test_measure_unknown_metadata_filename_produces_null_estimates(self):
        self.cur.metadata_file_override = "s3://lab/metadata/v121.metadata.json"
        for value in self.lesson.measure().values():
            for key in ("metadata_version", "metadata_json_written_est", "metadata_untracked_est"):
                self.assertIsNone(value[key])

    def test_measure_empty_metadata_log_produces_null_estimates(self):
        execute = self.cur.execute
        def empty_log(sql):
            execute(sql)
            if sql.startswith("SELECT file FROM"):
                self.cur.rows = []
        with patch.object(self.cur, "execute", side_effect=empty_log):
            for value in self.lesson.measure().values():
                for key in ("metadata_version", "metadata_json_written_est", "metadata_untracked_est"):
                    self.assertIsNone(value[key])

    def test_verify_reads_persisted_baseline_in_new_instance(self):
        self.grow(5)
        another = Lesson(self.cur, "s07-test")
        self.assertTrue(another.verify())
        self.assertEqual(another.records[-1]["checked"], 2)

    def test_verify_fails_for_missing_baseline(self):
        self.assertFalse(self.lesson.verify())
        self.assertFalse(self.lesson.records[-1]["match"])

    def test_verify_records_false_for_changed_rows_or_checksum(self):
        self.grow()
        self.cur.data["tuned"][0] += 1
        self.assertFalse(self.lesson.verify())
        self.assertEqual(self.lesson.records[-1]["step"], "verify")
        self.assertFalse(self.lesson.records[-1]["match"])

    def test_rows_or_checksum_change_fails_automatic_measure(self):
        for change in ("rows", "checksum"):
            with self.subTest(change=change):
                self.cur = FakeCursor()
                self.lesson = Lesson(self.cur, "s07-test")
                self.grow()
                if change == "rows":
                    self.cur.data["plain"].append(0)
                else:
                    self.cur.data["plain"][0] += 1
                with self.assertRaises(AssertionError):
                    self.lesson.manifests()
                self.assertFalse(self.lesson.measurements[-1]["tables"]["plain"]["match"])

    def test_verify_remembers_prior_measurement_failure_even_after_repair(self):
        self.grow()
        self.cur.data["plain"].append(0)
        with self.assertRaises(AssertionError):
            self.lesson.measure("compact")
        self.cur.data["plain"].pop()
        self.assertFalse(self.lesson.verify())

    def test_progress_trap_safe_success_unsafe_failure_and_sql_order(self):
        self.lesson.progress_trap()
        records = [r for r in self.lesson.records if r.get("cleanup")]
        self.assertEqual([r["cleanup"] for r in records], ["safe", "unsafe"])
        self.assertEqual([r["succeeded"] for r in records], [True, False])
        self.assertTrue(all(r["passed"] for r in records))
        self.assertEqual(len(records[-1]["error"]), 300)
        progress = 10000 - 37
        self.assertEqual(records[0]["progress"], progress)
        queries = self.cur.queries
        writes = [q for q in queries if q.startswith("INSERT")]
        self.assertEqual(len(writes), 6)
        self.assertTrue(all("_inc VALUES" in q for q in writes))
        expires = [q for q in queries if q.startswith("CALL lake.system.expire_snapshots")]
        self.assertEqual(len(expires), 2)
        self.assertIn("older_than => TIMESTAMP '2026-01-02 00:00:02', retain_last => 1", expires[0])
        calls = [q for q in queries if q.startswith("CALL lake.system.create_changelog_view")]
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[0], calls[1])
        self.assertIn("table => 'lesson07.s07_test_inc'", calls[0])
        self.assertIn(f"'start-snapshot-id','{progress}'", calls[0])
        self.assertIn("'end-snapshot-id','9815'", calls[0])
        self.assertIn("changelog_view => 's07_s07_test_changes_inc'", calls[0])
        self.assertIn("identifier_columns => array('event_id'), compute_updates => true", calls[0])
        self.assertLess(queries.index(expires[0]), queries.index(calls[0]))
        self.assertLess(queries.index(calls[0]), queries.index(expires[1]))
        self.assertFalse(any("DROP" in q for q in queries))
        self.assertEqual([m["after"] for m in self.lesson.measurements], ["progress-trap-safe", "progress-trap"])

    def test_progress_trap_catches_lazy_query_failure(self):
        self.cur.lazy_failure = True
        self.lesson.progress_trap()
        record = next(r for r in self.lesson.records if r.get("cleanup") == "unsafe")
        self.assertTrue(record["passed"])
        self.assertIn("조회 시점", record["error"])

    def test_progress_trap_rejects_wrong_safe_or_unsafe_outcome(self):
        for flag in ("safe_fails", "unsafe_succeeds"):
            self.cur = FakeCursor()
            setattr(self.cur, flag, True)
            lesson = Lesson(self.cur, "s07-test")
            with self.subTest(flag=flag), self.assertRaises(AssertionError):
                lesson.progress_trap()
            self.assertFalse(lesson.records[-1]["passed"])

    def test_progress_trap_refuses_repeated_inc_before_insert(self):
        self.cur.data["inc"] = [100]
        with self.assertRaises(ValueError):
            self.lesson.progress_trap()
        self.assertFalse(any(q.startswith("INSERT") for q in self.cur.queries))

    def test_all_order_and_every_stop_after(self):
        for stop in (*STEPS, None):
            called = []
            with contextlib.ExitStack() as stack:
                for step in STEPS:
                    stack.enter_context(patch.object(self.lesson, step.replace("-", "_"),
                                                     side_effect=lambda *args, step=step: called.append((step, args)) or True))
                self.lesson.all(commits=7, stop_after=stop)
            wanted = list(STEPS[:STEPS.index(stop) + 1]) if stop else list(STEPS)
            with self.subTest(stop=stop):
                self.assertEqual([step for step, _ in called], wanted)
                if "grow" in wanted:
                    self.assertEqual(dict(called)["grow"], (7,))

    def test_all_rejects_invalid_parameters_before_sql_and_failed_verify(self):
        with self.assertRaises(ValueError):
            self.lesson.all(stop_after="unknown")
        with self.assertRaises(ValueError):
            self.lesson.all(commits=0)
        self.assertEqual(self.cur.queries, [])
        with contextlib.ExitStack() as stack:
            for step in STEPS:
                stack.enter_context(patch.object(self.lesson, step.replace("-", "_"), return_value=False))
            with self.assertRaises(AssertionError):
                self.lesson.all()

    def test_full_all_emits_json_lines_and_uses_only_lesson07(self):
        self.lesson.all(commits=3)
        self.assertTrue(self.lesson.records[-1]["match"])
        lines = [json.loads(line) for line in self.output.getvalue().splitlines()]
        self.assertEqual(lines, json.loads(json.dumps(self.lesson.records, default=str)))
        self.assertTrue(all(r["run_id"] == "s07-test" for r in lines))
        for sql in self.cur.queries:
            self.assertNotRegex(sql, r"\b(?:lake\.)?(?:bronze|silver|gold|lesson04|lesson06)\b")

    def test_cli_validates_before_connection_and_dispatches(self):
        module = lab_module
        for args in (("all", "--run-id", "s06-invalid"),
                     ("grow", "--run-id", "s07-test", "--stop-after", "grow"),
                     ("grow", "--run-id", "s07-test", "--commits", "0")):
            with self.subTest(args=args), patch.object(module, "connect") as connect, \
                    patch.object(sys, "argv", ["lab.py", *args]), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises((ValueError, SystemExit)):
                    main()
                connect.assert_not_called()
        with patch.object(module, "connect") as connect, patch.object(Lesson, "all") as all_action, \
                patch.object(sys, "argv", ["lab.py", "all", "--run-id", "s07-test",
                                           "--commits", "5", "--stop-after", "grow"]):
            main()
            all_action.assert_called_once_with(5, "grow")


class DemoTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        (self.root / "labs/session07").mkdir(parents=True)
        (self.root / "scripts").mkdir()
        shutil.copy(ROOT / "labs/session07/demo.sh", self.root / "labs/session07/demo.sh")
        runner = self.root / "scripts/lab.sh"
        runner.write_text("#!/usr/bin/env python3\n" + textwrap.dedent(f"""
            import importlib.util
            import os
            import sys
            import types
            sys.path.insert(0, {str(ROOT)!r})
            if importlib.util.find_spec("pyhive") is None:
                sys.modules["pyhive"] = types.SimpleNamespace(hive=None)
            from labs.session07 import lab

            class Cursor:
                def execute(self, sql):
                    self.rows = []
                    if sql == "SHOW TABLES IN lake.lesson07" and os.environ.get("DEMO_EXISTING"):
                        self.rows = [("lesson07", "s07_demo_trap_inc", False)]
                def fetchall(self):
                    return self.rows

            class Connection:
                def __enter__(self):
                    return self
                def __exit__(self, *args):
                    pass
                def cursor(self):
                    return Cursor()

            class Lesson(lab.Lesson):
                def all(self, commits=120, stop_after=None):
                    self.record("all", commits=commits, stop_after=stop_after)

            lab.Lesson = Lesson
            lab.connect = Connection
            sys.argv = ["-", sys.argv[-1]]
            exec(compile(sys.stdin.read(), "demo-stdin", "exec"))
        """))
        runner.chmod(0o755)

    def run_demo(self, *args, existing=False):
        env = {**os.environ, "DEMO_EXISTING": "1" if existing else ""}
        return subprocess.run(["bash", str(self.root / "labs/session07/demo.sh"), *args],
                              cwd=self.root, env=env, capture_output=True, text=True)

    def test_five_fixed_runs_and_stop_stages(self):
        result = self.run_demo()
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([(r["run_id"], r["stop_after"], r["commits"]) for r in records],
                         [("s07-demo-grow", "grow", 120), ("s07-demo-compact", "compact", 120),
                          ("s07-demo-expire", "expire", 120), ("s07-demo-clean", "manifests", 120),
                          ("s07-demo-trap", "progress-trap", 5)])

    def test_last_run_collision_stops_before_table_changes(self):
        result = self.run_demo(existing=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("s07-demo-trap", result.stderr)
        self.assertIn("--fresh", result.stderr)

    def test_fresh_exact_cleanup_and_log_append(self):
        log = self.root / "output with spaces/demo.jsonl"
        first = self.run_demo("--fresh", "--log", str(log), existing=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertEqual(log.read_text(), first.stdout)
        cleans = [r for r in map(json.loads, first.stdout.splitlines()) if r["step"] == "clean"]
        self.assertEqual(len(cleans), 5)
        for record in cleans:
            prefix = "lake.lesson07." + record["run_id"].replace("-", "_") + "_"
            self.assertEqual(record["tables"], [prefix + suffix for suffix in ("plain", "tuned", "inc")])
        second = self.run_demo("--fresh", "--log", str(log), existing=True)
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(log.read_text(), first.stdout + second.stdout)

    def test_help_bad_arguments_and_runner_failure_exit(self):
        result = self.run_demo("--help")
        self.assertEqual(result.returncode, 0)
        self.assertIn("사용법", result.stdout)
        for args in (("--log",), ("--unknown",)):
            result = self.run_demo(*args)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(result.stdout, "")
        runner = self.root / "scripts/lab.sh"
        runner.write_text("#!/usr/bin/env bash\nexit 9\n")
        self.assertEqual(self.run_demo("--log", str(self.root / "failed.jsonl")).returncode, 9)


if __name__ == "__main__":
    unittest.main()
