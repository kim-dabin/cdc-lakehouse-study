"""Contract tests with fake Thrift I/O; never contact a real Spark session."""
import sys
import threading
import types
import unittest
from contextlib import closing
from unittest.mock import MagicMock, patch

from tools.explorer.app import spark

STATE = types.SimpleNamespace(INITIALIZED_STATE=0, RUNNING_STATE=1, FINISHED_STATE=2,
                              PENDING_STATE=7, ERROR_STATE=5)
TTYPES = types.SimpleNamespace(TOperationState=STATE)


class SparkTests(unittest.TestCase):
    def setUp(self):
        self.modules = patch.dict(sys.modules, {"TCLIService": types.ModuleType("TCLIService"),
                                               "TCLIService.ttypes": TTYPES})
        self.modules.start()
        self.addCleanup(self.modules.stop)
        self.slots = threading.BoundedSemaphore(2)
        self.slot_patch = patch.object(spark, "_SLOTS", self.slots)
        self.slot_patch.start()
        self.addCleanup(self.slot_patch.stop)

    def connection(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value
        cursor.poll.return_value = types.SimpleNamespace(operationState=STATE.FINISHED_STATE)
        cursor.description = [("value",)]
        cursor.fetchmany.return_value = [(1,), (2,), (3,)]
        return conn, cursor

    def test_reject_before_connection(self):
        with patch.object(spark, "_connect") as connect:
            for sql, rows in (("USE lake.gold", 10), ("SELECT 1", 501), ("SELECT 1", True)):
                with self.assertRaises(ValueError):
                    spark.query(sql, rows)
            connect.assert_not_called()

    def test_row_cap_new_connection_and_cleanup(self):
        connections = [self.connection(), self.connection()]
        with patch.object(spark, "_connect", side_effect=[c for c, _ in connections]):
            for conn, cursor in connections:
                result = spark.query("SELECT 1", 2)
                self.assertEqual(result["rows"], [(1,), (2,)])
                self.assertTrue(result["truncated"])
                cursor.execute.assert_called_once_with("SELECT 1", async_=True)
                cursor.fetchmany.assert_called_once_with(3)
                conn.cursor.assert_called_once_with(arraysize=3)
                # Cleanup is worker-owned and can finish just after the result is delivered.
                for _ in range(100):
                    if conn.close.call_count:
                        break
                    threading.Event().wait(.001)
                conn.close.assert_called_once()
                cursor.close.assert_called_once()

    def test_spark_error_message_and_cleanup(self):
        conn, cursor = self.connection()
        cursor.poll.return_value = types.SimpleNamespace(operationState=STATE.ERROR_STATE,
                                                       errorMessage="Spark original error")
        with patch.object(spark, "_connect", return_value=conn):
            with self.assertRaisesRegex(RuntimeError, "Spark original error"):
                spark.query("SELECT missing", 2)
        cursor.fetchmany.assert_not_called()

    def test_timeout_cancel_and_global_two_slot_limit(self):
        entered, release, closed = threading.Event(), threading.Event(), threading.Event()
        conn, cursor = self.connection()

        def execute(*args, **kwargs):
            entered.set()
            release.wait(2)

        cursor.execute.side_effect = execute
        conn.close.side_effect = closed.set
        self.slots.acquire()  # Model one other in-flight request.
        try:
            with patch.object(spark, "_connect", return_value=conn), patch.object(spark, "QUERY_TIMEOUT_SECONDS", .04), \
                    patch.object(spark, "SLOT_WAIT_SECONDS", .05):
                with self.assertRaises(TimeoutError):
                    spark.query("SELECT 1", 2)
                self.assertTrue(entered.is_set())
                with self.assertRaisesRegex(RuntimeError, "두 개"):
                    spark.query("SELECT 2", 2)
                release.set()
                self.assertTrue(closed.wait(1))
                self.assertGreaterEqual(cursor.cancel.call_count, 1)
                self.assertTrue(self.slots.acquire(timeout=1))
                self.slots.release()
        finally:
            release.set()
            self.slots.release()

    def test_pyhive_bootstrap_never_sends_use(self):
        sent = []

        class Cursor:
            def __init__(self, connection, *args, **kwargs):
                self._connection = connection
            def execute(self, operation, parameters=None, **kwargs):
                sent.append(operation)
            def close(self):
                pass

        class Connection:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                with closing(self.cursor()) as cursor:
                    cursor.execute("USE `default`")

        hive = types.SimpleNamespace(Cursor=Cursor, Connection=Connection)
        transport = types.SimpleNamespace(TSocket=MagicMock(), TTransport=MagicMock())
        with patch.dict(sys.modules, {"pyhive": types.SimpleNamespace(hive=hive),
                                      "thrift": types.ModuleType("thrift"),
                                      "thrift.transport": transport}):
            conn = spark._connect()
            self.assertEqual(sent, [])
            conn.cursor().execute("SELECT 1")
            self.assertEqual(sent, ["SELECT 1"])
            with self.assertRaises(ValueError):
                conn.cursor().execute("USE lake.gold")
            with self.assertRaises(ValueError):
                conn.cursor().execute("SET spark.x=1")
            self.assertNotIn("configuration", conn.kwargs)
