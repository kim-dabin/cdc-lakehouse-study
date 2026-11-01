import unittest
from tools.explorer.app.paths import parse_path, table_identifier


class PathTests(unittest.TestCase):
    def test_warehouse_paths(self):
        for scheme in ("s3", "s3a"):
            self.assertEqual(parse_path(f"{scheme}://warehouse/a/b file.parquet"),
                             ("warehouse", "a/b file.parquet"))
        self.assertEqual(parse_path("s3://warehouse/a%20b"), ("warehouse", "a%20b"))

    def test_rejected_paths(self):
        for path in ("", "a/b", "https://warehouse/a", "s3://other/a", "s3://warehouse/",
                     "s3://warehouse", "s3://warehouse.evil/a", "s3://u@warehouse/a",
                     "s3://warehouse/a?x=1", "s3://warehouse/a#b", "s3://warehouse/a?", "s3://warehouse/a#", "s3://warehouse/a\n"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                parse_path(path)

    def test_identifiers(self):
        self.assertEqual(table_identifier("silver", "refunds"), "lake.silver.refunds")
        self.assertEqual(table_identifier("_ns1"), "lake._ns1")
        for part in ("silver.x", "a;DROP", "`silver`", "1x", "", "a b", "한글"):
            with self.subTest(part=part), self.assertRaises(ValueError):
                table_identifier(part, "refunds")
            with self.assertRaises(ValueError):
                table_identifier("silver", part)
