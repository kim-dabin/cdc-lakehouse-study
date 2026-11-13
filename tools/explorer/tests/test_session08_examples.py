"""실제 UI의 8회차 SQL을 검사해요. Spark 실행 확인은 별도로 필요해요."""
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
            if line.strip().startswith('["8회차 ·')]


class Session08ExamplesTests(unittest.TestCase):
    def test_names_guard_and_exact_table_scope(self):
        examples = lesson_examples()
        self.assertEqual(len(examples), 8)
        self.assertEqual(len({name for name, _ in examples}), len(examples))
        for name, sql in examples:
            with self.subTest(name=name):
                validate_sql(sql)
                self.assertTrue(sql.startswith("SELECT ") or
                                sql == "SHOW TBLPROPERTIES lake.bronze.orders")
                self.assertNotRegex(sql, r"(?i)\b(CALL|JOIN|WITH)\b|\.changes|[;{}]")
                sources = re.findall(r"\bFROM ([\w.]+)", sql)
                if sql.startswith("SELECT "):
                    self.assertTrue(sources)
                for source in sources:
                    self.assertRegex(source, r"^lake\.(bronze\.(orders|payments|refunds)(\.(snapshots|metadata_log_entries))?|silver\.(orders|dim_order)|gold\.(mart_order_settlement|mart_daily_settlement))$")

    def test_all_example_names_are_unique(self):
        html = (ROOT / "tools/explorer/app/static/index.html").read_text()
        block = html.split("const examples=[", 1)[1].split("\n];", 1)[0]
        examples = [json.loads(line.strip().rstrip(",")) for line in block.splitlines()
                    if line.strip().startswith('["')]
        names = [name for name, _ in examples]
        self.assertTrue(names)
        self.assertEqual(len(names), len(set(names)))


if __name__ == "__main__":
    unittest.main()
