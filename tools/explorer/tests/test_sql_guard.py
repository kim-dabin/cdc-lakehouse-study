import unittest
from tools.explorer.app.sql_guard import FORBIDDEN, validate_sql


class SQLGuardTests(unittest.TestCase):
    def test_allowed(self):
        examples = [
            "SHOW NAMESPACES IN lake",
            *[f"SHOW TABLES IN lake.{ns}" for ns in ("bronze", "silver", "gold")],
            "SELECT * FROM lake.gold.mart_daily_settlement ORDER BY order_date",
            "SELECT * FROM lake.silver.refunds",
            "DESCRIBE TABLE EXTENDED lake.gold.fct_order_settlement",
            "SELECT committed_at, snapshot_id, parent_id, operation FROM lake.gold.fct_order_settlement.snapshots ORDER BY committed_at DESC LIMIT 10",
            "SELECT file_path, record_count, file_size_in_bytes FROM lake.silver.refunds.files",
            *[f"SELECT * FROM lake.silver.refunds.{suffix}" for suffix in ("manifests", "metadata_log_entries", "history")],
            "SELECT 'drop', 'a; INSERT', \"CREATE\", `USE`",
            "-- DROP\n/* INSERT /* nested */ SET */ select 1; -- end",
            "SELECT 'it''s drop', 'it\\'s drop', \"a\\\"drop\", `a``drop`",
            "select date_add(day, 1), is_deleted, replace('a', 'a', 'b') from t;",
            "WITH x AS (SELECT 1) SELECT * FROM x", "VALUES (1), (2)",
            "TABLE lake.silver.refunds", "EXPLAIN SELECT 1", "DESC t",
            "SHOW /* comment */ CREATE TABLE lake.silver.refunds;",
            "SELECT * FROM a JOIN b USING (id)", "SELECT transform(array(1, 2), x -> x + 1)",
            "SELECT 'reflect(', `java_method`, is_reflect FROM t",
        ]
        for sql in examples:
            with self.subTest(sql=sql):
                validate_sql(sql)

    def test_rejected(self):
        examples = [
            "INSERT INTO t SELECT 1", "WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x",
            "EXPLAIN INSERT INTO t SELECT 1", "SELECT 1; DROP TABLE t", "/* */ DROP TABLE t",
            "SET spark.x=1", "USE lake.gold", "CALL lake.system.expire_snapshots('t')",
            "CREATE TEMP VIEW v AS SELECT 1", "CACHE TABLE t", "REFRESH TABLE t",
            "", "   ", ";", ";;", "-- only comment", "SELECT " + "x" * 20_000,
            "SELECT 1;;", "SELECT 'unclosed", "SELECT `unclosed", "SELECT /* open",
            "'prefix' SELECT 1", "REPLACE TABLE t AS SELECT 1",
            "SHOW CREATE TABLE t; SELECT 1", "SHOW CREATE TABLE t DROP",
            "SELECT 1 /* comment */ ; /* comment */ ;",
            "SELECT 1 AS create", "SELECT 1 AS set", "SEL/**/ECT 1",
            "SELECT java_method('java.lang.System', 'exit', 0)",
            "SELECT reflect('java.lang.System', 'exit', 0)", "SELECT TRY_REFLECT ('java.lang.Thread', 'sleep', 1000L)",
            "SELECT TRANSFORM(a) USING 'cat' AS (b) FROM t", "SELECT transform(a) using \"sh -c id\" FROM t",
        ]
        for sql in examples:
            with self.subTest(sql=sql), self.assertRaises(ValueError):
                validate_sql(sql)

    def test_every_forbidden_word_and_word_boundaries(self):
        for word in FORBIDDEN:
            with self.subTest(word=word), self.assertRaises(ValueError):
                validate_sql(f"SELECT {word.lower()} FROM t")
            validate_sql(f"SELECT '{word}', `{word}`, is_{word.lower()} FROM t")


if __name__ == "__main__":
    unittest.main()
