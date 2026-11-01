"""Bounded, per-request NOSASL Thrift queries without shared-session mutations."""
import os
import threading
import time
from contextlib import suppress
from queue import Empty, Queue

from .sql_guard import validate_sql

MAX_ROWS = min(500, max(1, int(os.getenv("MAX_ROWS", "500"))))
QUERY_TIMEOUT_SECONDS = max(1, int(os.getenv("QUERY_TIMEOUT_SECONDS", "60")))
# 앞 조회가 연결을 닫는 동안 잠깐 자리가 비지 않으므로, 거절하지 않고 이 시간만큼 기다린다.
SLOT_WAIT_SECONDS = max(0.0, float(os.getenv("SLOT_WAIT_SECONDS", "20")))
_SLOTS = threading.BoundedSemaphore(2)


def _connect():
    from pyhive import hive
    from thrift.transport import TSocket, TTransport

    class ReadOnlyCursor(hive.Cursor):
        def execute(self, operation, parameters=None, **kwargs):
            # PyHive 0.7.0 issues USE `default` in Connection.__init__.
            # Suppress only that driver bootstrap statement, never send it to Spark.
            if self._connection._explorer_initializing and operation == "USE `default`":
                return None
            validate_sql(operation)
            return super().execute(operation, parameters, **kwargs)

    class ReadOnlyConnection(hive.Connection):
        def cursor(self, *args, **kwargs):
            return ReadOnlyCursor(self, *args, **kwargs)

    socket = TSocket.TSocket(os.getenv("SPARK_HOST", "spark"), int(os.getenv("SPARK_PORT", "10000")))
    socket.setTimeout(QUERY_TIMEOUT_SECONDS * 1000)
    transport = TTransport.TBufferedTransport(socket)  # PyHive's NOSASL transport
    conn = ReadOnlyConnection.__new__(ReadOnlyConnection)
    conn._explorer_initializing = True
    try:
        conn.__init__(username="lab", thrift_transport=transport)
    except BaseException:
        transport.close()
        raise
    finally:
        conn._explorer_initializing = False
    return conn


def query(sql: str, max_rows: int = 200) -> dict:
    validate_sql(sql)
    if isinstance(max_rows, bool) or not isinstance(max_rows, int) or not 1 <= max_rows <= MAX_ROWS:
        raise ValueError(f"표시할 행 수는 1부터 {MAX_ROWS}까지입니다.")
    if not _SLOTS.acquire(timeout=SLOT_WAIT_SECONDS):
        raise RuntimeError("조회 두 개가 실행 중이라 기다렸지만 자리가 나지 않았어요. 잠시 후 다시 실행해 주세요.")
    started = time.monotonic()
    deadline = started + QUERY_TIMEOUT_SECONDS
    result = Queue(maxsize=1)
    expired = threading.Event()
    state = {}
    rpc = threading.RLock()

    def work():
        conn = cursor = None
        try:
            conn = _connect()
            cursor = conn.cursor(arraysize=max_rows + 1)
            state["cursor"] = cursor
            if expired.is_set():
                return
            with rpc:
                cursor.execute(sql, async_=True)
            from TCLIService.ttypes import TOperationState
            while True:
                with rpc:
                    response = cursor.poll()
                    status = response.operationState
                if status not in (TOperationState.INITIALIZED_STATE, TOperationState.RUNNING_STATE,
                                  TOperationState.PENDING_STATE):
                    if status != TOperationState.FINISHED_STATE:
                        raise RuntimeError(getattr(response, "errorMessage", None)
                                           or f"Spark 조회가 종료됐습니다. 상태: {status}")
                    break
                if expired.wait(0.1) or time.monotonic() >= deadline:
                    raise TimeoutError("조회 시간이 초과됐습니다.")
            if expired.is_set():
                return
            with rpc:
                rows = cursor.fetchmany(max_rows + 1)
                columns = [c[0] for c in cursor.description or []]
            result.put((True, {"columns": columns,
                               "rows": rows[:max_rows], "truncated": len(rows) > max_rows,
                               "elapsed_ms": round((time.monotonic() - started) * 1000)}))
        except Exception as exc:
            result.put((False, exc))
        finally:
            if cursor is not None:
                with suppress(Exception):
                    if expired.is_set() or time.monotonic() >= deadline:
                        with rpc:
                            cursor.cancel()
                with suppress(Exception):
                    with rpc:
                        cursor.close()
            if conn is not None:
                with suppress(Exception):
                    with rpc:
                        conn.close()
            # Keep the slot until work and cleanup actually finish, even on timeout.
            _SLOTS.release()

    threading.Thread(target=work, daemon=True).start()
    try:
        ok, value = result.get(timeout=max(0, deadline - time.monotonic()))
    except Empty:
        expired.set()

        def cancel():
            with suppress(Exception):
                if state.get("cursor") is not None:
                    with rpc:
                        state["cursor"].cancel()

        threading.Thread(target=cancel, daemon=True).start()
        raise TimeoutError(f"{QUERY_TIMEOUT_SECONDS}초 안에 조회를 마치지 못했습니다.") from None
    if not ok:
        raise value
    return value
