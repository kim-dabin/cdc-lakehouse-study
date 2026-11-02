"""실제 UI의 6회차 SQL을 검사해요. Spark 실행 검증은 별도로 필요해요."""
import importlib.util
import json
from pathlib import Path
import re
import sqlite3
import sys
import types
import unittest
from unittest.mock import patch

from tools.explorer.app.sql_guard import validate_sql

if importlib.util.find_spec("pyhive") is None:
    with patch.dict(sys.modules, {"pyhive": types.SimpleNamespace(hive=None)}):
        from labs.session06.lab import expected, fixture_state
else:
    from labs.session06.lab import expected, fixture_state

ROOT = Path(__file__).resolve().parents[3]


def lesson_examples():
    html = (ROOT / "tools/explorer/app/static/index.html").read_text()
    block = html.split("const examples=[", 1)[1].split("\n];", 1)[0]
    return [json.loads(line.strip().rstrip(",")) for line in block.splitlines()
            if line.strip().startswith('["6회차 ·')]


class Session06ExamplesTests(unittest.TestCase):
    def test_all_lesson_examples_pass_guard_and_use_fixed_demo_tables(self):
        examples = lesson_examples()
        self.assertEqual(len(examples), 12)
        self.assertEqual(len({name for name, _ in examples}), len(examples))
        for name, sql in examples:
            with self.subTest(name=name):
                validate_sql(sql)
                self.assertRegex(sql, r"^(SELECT|WITH)\b")
                self.assertRegex(sql, r"lake\.lesson06\.s06_demo_(s[2-6]|naive)_")
                self.assertNotIn(";", sql)
                self.assertNotIn("{", sql)

    def test_raw_changes_shows_all_commits_and_file_evidence_is_available(self):
        queries = dict(lesson_examples())
        raw = queries["6회차 · s5 원본 changes · o2 carry-over"]
        self.assertEqual(raw, "SELECT _change_type, _change_ordinal, _commit_snapshot_id, "
                         "order_id, ordered_at, source_lsn FROM lake.lesson06.s06_demo_s5_orders.changes "
                         "ORDER BY _change_ordinal, order_id, _change_type")
        self.assertIn("operation", queries["6회차 · s5 파일 재작성 snapshot"])
        self.assertIn("file_path, record_count", queries["6회차 · s5 현재 데이터 파일"])

    def test_all_raw_changes_projections_pair_snapshot_with_ordinal(self):
        html = (ROOT / "tools/explorer/app/static/index.html").read_text()
        projections = re.findall(
            r"\bSELECT\s+([^\n\"'`]*?)\s+FROM\s+[\w.]+\.changes\b", html, re.I)
        self.assertTrue(projections)
        for projection in projections:
            with self.subTest(projection=projection):
                columns = [column.strip().lower() for column in projection.split(",")]
                if "_commit_snapshot_id" in columns:
                    self.assertIn("_change_ordinal", columns)
                    self.assertLess(columns.index("_change_ordinal"),
                                    columns.index("_commit_snapshot_id"))

    def test_metadata_examples_are_simple_selects_without_subqueries(self):
        for name, sql in lesson_examples():
            if re.search(r"\.(changes|history|snapshots|refs|files)\b", sql):
                with self.subTest(name=name):
                    self.assertEqual(len(re.findall(r"\bSELECT\b", sql, re.I)), 1)
                    self.assertNotRegex(sql.upper(), r"\b(WITH|JOIN|WHERE|IN)\b")

    def test_inlined_full_calculation_follows_core_and_full_templates(self):
        query = dict(lesson_examples())["6회차 · s6 naive와 전체 재계산 차이"]
        names = {n: n for n in ("dim_order", "fct_payment", "fct_refund", "full_order", "full_daily")}
        names.update({f"input_{t}": f"lake.lesson06.s06_demo_naive_{t}"
                      for t in ("orders", "payments", "refunds")})
        normalize = lambda s: " ".join(s.split())
        for filename in ("01-core.sql", "02-full.sql"):
            template = (ROOT / "labs/session06/sql" / filename).read_text().format(**names)
            template = re.sub(r"(?m)^--[^\n]*\n?", "", template)
            for statement in template.split(";"):
                if statement.strip():
                    match = re.fullmatch(r"\s*CREATE OR REPLACE TEMP VIEW (\w+) AS\s*(.*?)\s*", statement, re.S)
                    self.assertIsNotNone(match)
                    self.assertIn(normalize(f"{match[1]} AS ( {match[2]} )"), normalize(query))

    def test_naive_difference_returns_stranded_october_28_row(self):
        # SQLite로 관계 연산·고정 데이터 결과만 확인해요. Spark 파서/파일 검증은 아니에요.
        query = dict(lesson_examples())["6회차 · s6 naive와 전체 재계산 차이"]
        query = query.replace("lake.lesson06.s06_demo_naive_", "")
        with sqlite3.connect(":memory:") as conn:
            conn.create_function("from_utc_timestamp", 2, lambda value, zone: value)
            conn.create_function("to_date", 1, lambda value: value[:10])
            for table, rows in fixture_state(6).items():
                columns = list(rows[0])
                conn.execute(f"CREATE TABLE {table} ({','.join(columns)})")
                conn.executemany(f"INSERT INTO {table} VALUES ({','.join('?' for _ in columns)})",
                                 [[r[c] for c in columns] for r in rows])
            conn.execute("CREATE TABLE gold_daily (order_date,order_count,paid,refunded,net_paid)")
            conn.executemany("INSERT INTO gold_daily VALUES (?,?,?,?,?)", expected(6)["gold_daily"])
            self.assertEqual(conn.execute(query).fetchall(), [])
            stranded = expected(5)["gold_daily"][0]
            conn.execute("INSERT INTO gold_daily VALUES (?,?,?,?,?)", stranded)
            self.assertEqual(conn.execute(query).fetchall(), [("actual_minus_full", *stranded)])


if __name__ == "__main__":
    unittest.main()
