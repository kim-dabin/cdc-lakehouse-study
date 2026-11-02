"""Spark 없이 실행 순서·범위·재시도 계약을 확인해요."""
import contextlib
import importlib.util
import io
import json
import re
import sys
import types
import unittest
from unittest.mock import patch

# 연결은 쓰지 않아요. 로컬에 드라이버가 없어도 SQL 계약을 검사해요.
if importlib.util.find_spec("pyhive") is None:
    with patch.dict(sys.modules, {"pyhive": types.SimpleNamespace(hive=None)}):
        from labs.session06.lab import InjectedFailure, Lesson, expected, fixture_state
else:
    from labs.session06.lab import InjectedFailure, Lesson, expected, fixture_state


class FakeCursor:
    """정해진 조회 응답과 쓰기 시점을 기록해요. Spark SQL 실행기는 아니에요."""
    def __init__(self):
        self.queries = []
        self.rows = []
        self.start = {"orders": 10, "payments": 20, "refunds": 30}
        self.end = {"orders": 11, "payments": 21, "refunds": 31}
        self.saved = None
        self.saved_range = None
        self.logged = {"orders": [("o1",)], "payments": [("p1",)], "refunds": [("r1",)]}
        self.different = dict(self.logged)
        self.raw = {t: [(row[0], "INSERT", 0, self.end[t]) for row in rows]
                    for t, rows in self.logged.items()}
        self.history = {t: [(self.start[t], 1, True), (self.end[t], 2, True)]
                        for t in self.end}
        self.logged_rows = {t: list(rows) for t, rows in self.logged.items()}
        self.old_date = "2026-10-28"
        self.new_date = "2026-10-25"
        self.current_gold_date = self.old_date
        self.existing = []
        self.except_counts = iter([0] * 20)

    def execute(self, sql):
        sql = sql.strip()
        self.queries.append(sql)
        sql = re.sub(r"(?m)^--[^\n]*\n?", "", sql).strip()
        self.rows = []
        if sql.startswith("SHOW TABLES"):
            self.rows = self.existing
        elif sql.startswith("SELECT input_table,snapshot_id"):
            self.rows = list(self.start.items())
        elif sql.startswith("SELECT snapshot_id FROM") and ".refs" in sql:
            table = next(t for t in self.end if f"_{t}.refs" in sql)
            self.rows = [] if self.end[table] is None else [(self.end[table],)]
        elif sql.startswith("SELECT ranges_json,keys_json,dates_json"):
            self.rows = [self.saved] if self.saved and self.saved_range in sql else []
        elif sql.startswith("WITH b AS"):
            table = next(t for t in self.end if f"_{t} " in sql)
            self.rows = self.different[table]
        elif sql.startswith("SELECT DISTINCT"):
            table = next(t for t in self.end if f"changes_{t}" in sql)
            self.rows = self.logged[table]
        elif sql.startswith("SELECT snapshot_id, made_current_at, is_current_ancestor FROM"):
            table = next(t for t in self.end if f"_{t}.history" in sql)
            self.rows = self.history[table]
        elif re.fullmatch(r"SELECT \w+, _change_type, _change_ordinal, _commit_snapshot_id FROM lake\.lesson06\.s06_test_\w+\.changes", sql):
            table = next(t for t in self.end if f"_{t}.changes" in sql)
            self.rows = self.raw[table]
        elif re.fullmatch(r"SELECT \w+ FROM s06_s06_test_changes_\w+", sql):
            table = next(t for t in self.end if f"changes_{t}" in sql)
            self.rows = self.logged_rows[table]
        elif sql.startswith("SELECT order_id FROM") and "affected_keys" in sql:
            self.rows = [("o1",)]
        elif sql.startswith("SELECT g.order_date"):
            dates = {self.current_gold_date, self.new_date}
            if "UPDATE_BEFORE" in sql:
                dates.add(self.old_date)
            self.rows = [(d,) for d in sorted(dates)]
        elif sql.startswith("INSERT INTO") and "_plan " in sql:
            values = re.findall(r"'((?:''|[^'])*)'", sql)
            self.saved_range = values[0]
            self.saved = tuple(v.replace("''", "'") for v in values[1:4])
        elif sql.startswith("MERGE INTO") and "_gold_order " in sql:
            self.current_gold_date = self.new_date
        elif sql.startswith("MERGE INTO") and "_progress " in sql:
            self.start = dict(self.end)
        elif sql.startswith("SELECT COUNT(*)") and " EXCEPT " in sql:
            self.rows = [(next(self.except_counts),)]

    def fetchall(self):
        return self.rows


class LabTests(unittest.TestCase):
    def setUp(self):
        self.output = io.StringIO()
        self.quiet = contextlib.redirect_stdout(self.output)
        self.quiet.__enter__()
        self.addCleanup(self.quiet.__exit__, None, None, None)
        self.cur = FakeCursor()
        self.lesson = Lesson(self.cur, "s06-test")

    def test_run_id_validation_before_any_sql(self):
        for run_id in ("s06-a", "s06-user_01", "s06-" + "a" * 36):
            with self.subTest(run_id=run_id):
                Lesson(self.cur, run_id)
        for run_id in ("", "s06-", "s04-a", "S06-a", "s06-A", "s06-a.b", "s06-a;DROP", "s06-a/b", "s06-a\n", "s06-" + "a" * 37):
            with self.subTest(run_id=run_id), self.assertRaises(ValueError):
                Lesson(self.cur, run_id)
        self.assertEqual(self.cur.queries, [])

    def test_prepare_all_tables_are_isolated_and_cow(self):
        self.lesson.prepare()
        creates = [q for q in self.cur.queries if q.startswith("CREATE TABLE")]
        self.assertEqual(len(creates), 7)
        for sql in creates:
            self.assertIn("lake.lesson06.s06_test_", sql)
            for operation in ("merge", "update", "delete"):
                self.assertIn(f"'write.{operation}.mode'='copy-on-write'", sql)
        for column in ("is_deleted", "source_lsn", "kafka_offset", "source_ts_ms"):
            self.assertIn(column, creates[0])

    def test_prepare_collision_stops_before_creating_tables(self):
        self.cur.existing = [("lesson06", "s06_test_orders", False)]
        with self.assertRaises(ValueError):
            self.lesson.prepare()
        self.assertFalse(any(q.startswith("CREATE TABLE") for q in self.cur.queries))
        self.assertEqual(Lesson(None, "s06-a-b").names["orders"], Lesson(None, "s06-a_b").names["orders"])

    def test_clean_only_drops_the_exact_generated_tables(self):
        # 카탈로그에 이름이 비슷한 테이블이 있어도 탐색하거나 삭제하지 않아요.
        self.cur.existing = [("lesson06", "s06_test_orders_backup", False),
                             ("lesson06", "s06_other_orders", False)]
        self.lesson.clean()
        names = [f"lake.lesson06.s06_test_{suffix}" for suffix in
                 ("orders", "payments", "refunds", "gold_order", "gold_daily", "progress", "plan")]
        self.assertEqual(self.cur.queries, [f"DROP TABLE IF EXISTS {name} PURGE" for name in names])
        self.assertEqual(self.lesson.records[-1]["tables"], names)
        self.assertIn("7개만", self.lesson.records[-1]["message"])

    def test_each_stage_batches_changed_rows_once_per_input_table(self):
        for stage in range(1, 7):
            with self.subTest(stage=stage), \
                    patch.object(self.lesson, "assert_fixture"), \
                    patch.object(self.lesson, "verify", return_value=True), \
                    patch.object(self.lesson, "check_expected", return_value=True):
                self.cur.queries.clear()
                self.lesson.scenario(f"s{stage}")
                before, after = fixture_state(stage - 1), fixture_state(stage)
                for table in before:
                    changed = [row for row in after[table] if row not in before[table]]
                    merges = [q for q in self.cur.queries
                              if q.startswith(f"MERGE INTO {self.lesson.names[table]} ")]
                    self.assertEqual(len(merges), int(bool(changed)))
                    if changed:
                        sql = merges[0]
                        self.assertIn("SELECT /*+ REPARTITION(1) */ * FROM (SELECT ", sql)
                        self.assertEqual(sql.count(" UNION ALL "), len(changed) - 1)
                        self.assertIn("s.source_lsn>t.source_lsn", sql)
                        self.assertIn("s.source_lsn=t.source_lsn AND s.kafka_offset>t.kafka_offset", sql)
                        if stage == 1 and table == "orders":
                            self.assertIn("CAST('o1' AS STRING) order_id", sql)
                            self.assertIn("CAST('o2' AS STRING) order_id", sql)

    def test_carryover_counts_share_bounds_and_do_not_change_crosscheck(self):
        self.cur.raw["orders"] = [("o1", "DELETE", 0, 11), ("o1", "INSERT", 0, 11),
                                  ("o2", "DELETE", 0, 11), ("o2", "INSERT", 0, 11)]
        self.cur.logged_rows["orders"] = [("o1",), ("o1",)]
        self.lesson.incremental()
        record = next(r for r in self.lesson.records if r["step"] == "carryover" and r["table"] == "orders")
        self.assertEqual(record, {"step": "carryover", "table": "orders", "start": 10, "end": 11,
                                  "raw_rows": 4, "raw_keys": ["o1", "o2"],
                                  "changelog_rows": 2, "changelog_keys": ["o1"], "carryover_keys": ["o2"]})
        crosscheck = next(r for r in self.lesson.records if r["step"] == "crosscheck" and r["table"] == "orders")
        self.assertTrue(crosscheck["match"])
        self.assertIn("SELECT snapshot_id, made_current_at, is_current_ancestor FROM "
                      "lake.lesson06.s06_test_orders.history", self.cur.queries)
        self.assertIn("SELECT order_id, _change_type, _change_ordinal, _commit_snapshot_id FROM "
                      "lake.lesson06.s06_test_orders.changes", self.cur.queries)

    def test_raw_changes_selects_ordinal_with_snapshot_and_filters_snapshot_column(self):
        for table, logged in self.cur.logged.items():
            with self.subTest(table=table):
                end = self.cur.end[table]
                key = logged[0][0]
                # ordinal이 end와 같아도 범위 밖 snapshot이면 제외해야 해요.
                self.cur.raw[table] = [("outside", "INSERT", end, self.cur.start[table]),
                                       (key, "INSERT", 0, end)]
                self.cur.queries.clear()
                self.lesson.crosscheck(table, self.cur.start[table], end,
                                       self.lesson.names["changes_" + table])
                queries = [q for q in self.cur.queries if ".changes" in q]
                self.assertTrue(queries)
                for query in queries:
                    projection = query.split(" FROM ", 1)[0]
                    self.assertIn("_change_ordinal, _commit_snapshot_id", projection)
                record = self.lesson.records[-2]
                self.assertEqual(record["raw_rows"], 1)
                self.assertEqual(record["raw_keys"], [key])

    def test_empty_carryover_range_avoids_raw_scan(self):
        self.cur.logged["orders"] = self.cur.different["orders"] = []
        self.cur.logged_rows["orders"] = []
        self.lesson.crosscheck("orders", 11, 11, self.lesson.names["changes_orders"])
        record = next(r for r in self.lesson.records if r["step"] == "carryover")
        self.assertEqual(record["raw_rows"], 0)
        self.assertEqual(record["changelog_rows"], 0)
        self.assertEqual(record["carryover_keys"], [])
        self.assertFalse(any(".changes" in q or ".history" in q for q in self.cur.queries))

    def test_raw_range_uses_history_order_and_current_ancestors(self):
        # 조회 순서·ID 숫자 크기와 무관하게 시간순 범위를 계산해요.
        self.cur.history["orders"] = [(100, 4, True), (900, 1, True), (50, 5, True),
                                      (800, 2, True), (700, 3, False)]
        self.cur.raw["orders"] = [("old", "INSERT", 0, 900), ("start", "INSERT", 0, 800),
                                  ("branch", "INSERT", 0, 700), ("o1", "DELETE", 0, 100),
                                  ("o1", "INSERT", 0, 100), ("o2", "DELETE", 0, 100),
                                  ("o2", "INSERT", 0, 100), ("future", "INSERT", 0, 50)]
        for start, end, count, keys in ((800, 100, 4, ["o1", "o2"]),
                                         (None, 800, 2, ["old", "start"]),
                                         (None, None, 0, []),
                                         (100, 100, 0, [])):
            with self.subTest(start=start, end=end):
                self.cur.queries.clear()
                self.lesson.crosscheck("orders", start, end, self.lesson.names["changes_orders"])
                record = self.lesson.records[-2]
                self.assertEqual(record["raw_rows"], count)
                self.assertEqual(record["raw_keys"], keys)
                for query in self.cur.queries:
                    if ".changes" in query:
                        self.assertEqual(query, "SELECT order_id, _change_type, _change_ordinal, _commit_snapshot_id "
                                         "FROM lake.lesson06.s06_test_orders.changes")

    def test_changelog_plan_and_progress_write_order(self):
        self.lesson.incremental()
        queries = self.cur.queries
        calls = [q for q in queries if "CALL lake.system.create_changelog_view" in q]
        self.assertEqual(len(calls), 3)
        for call in calls:
            self.assertIn("'start-snapshot-id'", call)
            self.assertIn("'end-snapshot-id'", call)
            self.assertIn("identifier_columns => array(", call)
            self.assertIn("compute_updates => true", call)
            self.assertIn("table => 'lesson06.", call)
        index = lambda text: next(i for i, q in enumerate(queries) if text in q)
        self.assertLess(index("INSERT INTO lake.lesson06.s06_test_plan"), index("MERGE INTO lake.lesson06.s06_test_gold_order"))
        self.assertLess(index("MERGE INTO lake.lesson06.s06_test_gold_order"), index("MERGE INTO lake.lesson06.s06_test_gold_daily"))
        self.assertLess(index("MERGE INTO lake.lesson06.s06_test_gold_daily"), index("MERGE INTO lake.lesson06.s06_test_progress"))
        self.assertEqual(self.cur.start, self.cur.end)
        self.assertEqual(json.loads(self.cur.saved[1]), ["o1"])
        self.assertEqual(json.loads(self.cur.saved[2]), ["2026-10-25", "2026-10-28"])
        self.assertTrue(all(r["match"] for r in self.lesson.records if r["step"] == "crosscheck"))
        for line in self.output.getvalue().splitlines():
            self.assertIn("step", json.loads(line))

    def test_changelog_does_not_drop_view_before_creation(self):
        for _ in range(2):
            self.lesson.changelog("orders", start=10, end=11)
        self.assertFalse(any(q.startswith("DROP VIEW") for q in self.cur.queries))
        self.assertEqual(len(self.cur.queries), 2)
        self.assertTrue(all(q.startswith("CALL lake.system.create_changelog_view")
                            for q in self.cur.queries))

    def test_first_range_omits_start_and_empty_range_does_not_call(self):
        self.cur.start = {}
        self.lesson.incremental()
        calls = [q for q in self.cur.queries if q.startswith("CALL")]
        self.assertEqual(len(calls), 3)
        self.assertTrue(all("start-snapshot-id" not in q for q in calls))
        self.cur.queries.clear()
        self.cur.logged = {t: [] for t in self.cur.end}
        self.cur.different = dict(self.cur.logged)
        self.lesson.incremental()
        self.assertFalse(any(q.startswith("CALL") for q in self.cur.queries))

    def test_failure_keeps_progress_and_retry_reuses_plan_without_changelog(self):
        original = dict(self.cur.start)
        with self.assertRaises(InjectedFailure):
            self.lesson.incremental(fail_after="order")
        self.assertEqual(self.cur.start, original)
        self.assertIsNotNone(self.cur.saved)
        self.assertFalse(any("MERGE INTO lake.lesson06.s06_test_progress" in q for q in self.cur.queries))
        self.assertFalse(any("MERGE INTO lake.lesson06.s06_test_gold_daily" in q for q in self.cur.queries))
        saved = self.cur.saved
        self.cur.queries.clear()
        self.lesson.incremental()
        self.assertEqual(self.cur.saved, saved)
        self.assertFalse(any(q.startswith("CALL") for q in self.cur.queries))
        self.assertFalse(any(q.startswith("INSERT INTO") for q in self.cur.queries))
        plan = [r for r in self.lesson.records if r["step"] == "plan"][-1]
        self.assertTrue(plan["reused"])
        self.assertIn(self.cur.old_date, plan["dates"])
        self.assertEqual(self.cur.start, self.cur.end)

    def test_naive_retry_loses_old_date_and_never_saves_plan(self):
        with self.assertRaises(InjectedFailure):
            self.lesson.incremental(fail_after="order", naive_dates=True)
        self.lesson.incremental(naive_dates=True)
        self.assertIsNone(self.cur.saved)
        plans = [r for r in self.lesson.records if r["step"] == "plan"]
        self.assertIn(self.cur.old_date, plans[0]["dates"])
        self.assertNotIn(self.cur.old_date, plans[1]["dates"])
        self.assertEqual(sum(q.startswith("CALL") for q in self.cur.queries), 6)

    def test_new_input_end_cannot_reuse_previous_range_plan(self):
        with self.assertRaises(InjectedFailure):
            self.lesson.incremental(fail_after="order")
        previous_range = self.cur.saved_range
        self.cur.end["orders"] = 12
        self.cur.queries.clear()
        self.lesson.incremental()
        self.assertNotEqual(self.cur.saved_range, previous_range)
        self.assertEqual(sum(q.startswith("CALL") for q in self.cur.queries), 3)
        self.assertFalse([r for r in self.lesson.records if r["step"] == "plan"][-1]["reused"])

    def test_s6_orchestrates_same_range_failure_retry_and_expected_record(self):
        with patch.object(self.lesson, "assert_fixture") as prerequisite, \
                patch.object(self.lesson, "verify", return_value=True) as verify, \
                patch.object(self.lesson, "check_expected", return_value=True) as fixed:
            self.lesson.scenario("s6")
        prerequisite.assert_called_once_with(5)
        fixed.assert_called_once_with(6)
        self.assertEqual(verify.call_count, 2)
        retry = next(r for r in self.lesson.records if r["step"] == "retry")
        self.assertTrue(retry["progress_unchanged"])
        plans = [r for r in self.lesson.records if r["step"] == "plan"]
        self.assertEqual(plans[0]["ranges"], plans[1]["ranges"])
        self.assertTrue(plans[1]["reused"])
        scenario = next(r for r in self.lesson.records if r["step"] == "scenario")
        self.assertEqual(scenario["expected"], expected(6))
        self.assertTrue(self.lesson.records[-1]["passed"])

    def test_mismatched_keys_stop_before_plan_or_gold_write(self):
        self.cur.logged["refunds"] = []
        with self.assertRaises(AssertionError):
            self.lesson.incremental()
        self.assertFalse(any(q.startswith(("MERGE INTO", "INSERT INTO")) for q in self.cur.queries))
        self.assertFalse(self.lesson.records[-1]["match"])

    def test_time_travel_compares_delete_marker_and_both_versions(self):
        self.lesson.incremental()
        diff = next(q for q in self.cur.queries if q.startswith("WITH b AS") and "_refunds " in q)
        self.assertIn("VERSION AS OF 30", diff)
        self.assertIn("VERSION AS OF 31", diff)
        self.assertIn("FULL OUTER JOIN", diff)
        self.assertIn("b.is_deleted <=> a.is_deleted", diff)
        affected = next(q for q in self.cur.queries if "AS\n          SELECT order_id" in q)
        self.assertIn("changes_refunds", affected)
        self.assertIn("input_payments", affected)
        self.assertNotIn("is_deleted", affected)
        self.assertNotIn("_change_type", affected)

    def test_verify_records_both_except_directions_and_mismatch(self):
        self.cur.except_counts = iter([0, 0, 1, 0])
        self.assertFalse(self.lesson.verify())
        record = self.lesson.records[-1]
        self.assertTrue(record["gold_order"]["match"])
        self.assertEqual(record["gold_daily"]["actual_minus_full"], 1)
        queries = [q for q in self.cur.queries if " EXCEPT " in q]
        self.assertEqual(len(queries), 4)
        self.assertTrue(all("mart_built_at" not in q for q in queries))

    def test_fixed_expected_amounts_and_old_new_dates(self):
        self.assertEqual(expected(1)["gold_order"][0][2:], [100000, 20000, 80000])
        self.assertEqual(expected(3)["gold_order"][0][2:], [100000, 30000, 70000])
        self.assertEqual(expected(4)["gold_order"][0][2:], [100000, 0, 100000])
        self.assertEqual(expected(5)["gold_daily"][0][0], "2026-10-28")
        self.assertEqual(expected(6)["gold_daily"][0][0], "2026-10-25")
        self.assertEqual(expected(6)["gold_daily"][1], ["2026-11-02", 2, 50000, 0, 50000])


if __name__ == "__main__":
    unittest.main()
