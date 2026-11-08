"""실제 UI의 7회차 SQL을 검사해요. Spark 실행 확인은 별도로 필요해요."""
import json
from pathlib import Path
import re
import unittest

from tools.explorer.app.sql_guard import validate_sql

ROOT = Path(__file__).resolve().parents[3]


def lesson_examples():
    html = (ROOT / "tools/explorer/app/static/index.html").read_text()
    block = html.split("const examples=[", 1)[1].split("\n];", 1)[0]
    return [json.loads(line.strip().rstrip(",")) for line in block.splitlines()
            if line.strip().startswith('["7회차 ·')]


class Session07ExamplesTests(unittest.TestCase):
    def test_names_guard_and_exact_demo_table_scope(self):
        examples = lesson_examples()
        self.assertEqual(len(examples), 8)
        self.assertEqual(len({name for name, _ in examples}), len(examples))
        seen = set()
        for name, sql in examples:
            with self.subTest(name=name):
                validate_sql(sql)
                self.assertTrue(sql.startswith("SELECT "))
                self.assertNotRegex(sql, r"(?i)\b(CALL|WITH|JOIN|WHERE)\b|\.changes\b|[;{}]")
                sources = re.findall(r"\bFROM ([\w.]+)", sql)
                self.assertTrue(sources)
                for source in sources:
                    self.assertRegex(source, r"^lake\.lesson07\.s07_demo_(grow|compact|expire|clean|trap)_(plain|tuned|inc)\.(snapshots|metadata_log_entries|files|all_data_files|manifests|all_manifests)$")
                    seen.add(source.split("_")[2])
        self.assertEqual(seen, {"grow", "compact", "expire", "clean", "trap"})

    def test_union_comparisons_pair_plain_and_tuned(self):
        queries = dict(lesson_examples())
        for name, metrics in (
            ("7회차 · grow snapshot·메타데이터 비교", ("snapshots", "metadata_log_entries")),
            ("7회차 · compact 현재 파일과 과거 파일", ("files", "all_data_files")),
            ("7회차 · expire 남은 snapshot과 파일", ("snapshots", "all_data_files")),
            ("7회차 · clean 현재·전체 매니페스트", ("manifests", "all_manifests")),
        ):
            sql = queries[name]
            with self.subTest(name=name):
                self.assertEqual(sql.count(" UNION ALL "), 3)
                self.assertEqual(sql.count("SELECT "), 4)
                for table in ("plain", "tuned"):
                    for metric in metrics:
                        self.assertEqual(sql.count(f"_{table}.{metric}"), 1)

    def test_all_file_counts_use_distinct_paths_and_explicit_columns(self):
        checked = 0
        for name, sql in lesson_examples():
            for select in sql.split(" UNION ALL "):
                for suffix, column in (("all_data_files", "file_path"), ("all_manifests", "path")):
                    if select.endswith(f".{suffix}"):
                        with self.subTest(name=name, select=select):
                            self.assertIn(f"'{suffix}_distinct' metric", select)
                            self.assertIn(f"count(DISTINCT {column}) distinct_count", select)
                            self.assertNotIn("count(*)", select)
                            self.assertIn("distinct_count", sql.split(" UNION ALL ")[0])
                        checked += 1
        self.assertEqual(checked, 6)

    def test_metadata_comparison_uses_only_tracked_log_count(self):
        for name, sql in lesson_examples():
            with self.subTest(name=name):
                self.assertNotRegex(name + " " + sql, r"(?i)orphan|고아|spark\.testing")
                self.assertNotRegex(sql, r"metadata_(?:version|json_written_est|untracked_est|json_est)")
        sql = dict(lesson_examples())["7회차 · grow snapshot·메타데이터 비교"]
        self.assertEqual(sql.count("'metadata_log' metric"), 2)
        self.assertEqual(sql.count(".metadata_log_entries"), 2)

    def test_history_sizes_and_retained_metadata_columns(self):
        queries = dict(lesson_examples())
        history = queries["7회차 · grow plain 최근 snapshot 5개"]
        self.assertIn("committed_at, operation, summary['added-records']", history)
        self.assertTrue(history.endswith("ORDER BY committed_at DESC LIMIT 5"))
        self.assertIn("count(*) data_files, avg(file_size_in_bytes) avg_bytes",
                      queries["7회차 · grow plain 데이터 파일 크기"])
        self.assertIn("trap_inc.snapshots", queries["7회차 · trap inc 남은 snapshot"])
        metadata = queries["7회차 · grow tuned 메타데이터 이력 · 최대 11개"]
        self.assertIn("timestamp, file, latest_snapshot_id", metadata)
        self.assertIn("grow_tuned.metadata_log_entries", metadata)
        # LIMIT로 잘라 11개처럼 보이게 만들지 않고 전체를 조회해요.
        self.assertNotIn("LIMIT", metadata)


if __name__ == "__main__":
    unittest.main()
