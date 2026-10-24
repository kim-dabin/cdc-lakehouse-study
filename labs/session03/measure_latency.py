"""Measure one explicit refund write to its first Bronze query observation.

Run in the existing runner with measure-latency.sh. No reset, restart, DML
retry, offset commit, cleanup, or mart refresh is performed. Without
--insert-refund this module prints the plan without opening connections.
"""
import argparse
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import json
import os
import re
import socket
import sys
import threading
import time


CASE_PATTERN = r"s01-[a-z0-9-]{3,48}"
TABLE = "lake.bronze.cdc_events"
TOPIC = "cdc.public.refunds"
SCOPE = (
    "Client observations from one warmed runner process and one monotonic clock. "
    "t1 is a commit response acknowledgement, not the database commit instant. "
    "The durations include polling, refresh, query and response costs. The "
    "observation interval is not a proven Iceberg commit-time interval. Kafka "
    "and Flink wall-clock columns are reference values, not clock differences. "
    "No checkpoint-to-snapshot-to-event mapping or SLA is established."
)


class MeasurementFailure(Exception):
    """A controlled error code that contains no connection credentials."""


class Clock:
    def monotonic(self):
        return time.monotonic()

    def utc(self):
        return datetime.now(timezone.utc).isoformat()

    def sleep(self, seconds):
        time.sleep(seconds)


def point(clock):
    # Capture the monotonic mark first; UTC is a presentation reference only.
    return {"monotonic_seconds": clock.monotonic(), "utc": clock.utc()}


def amount_is(value, expected):
    if value is None or isinstance(value, bool):
        return False
    try:
        number = Decimal(str(value))
        return number.is_finite() and number == expected
    except (InvalidOperation, ValueError, TypeError):
        return False


class Measurement:
    def __init__(self, case_id, run_label, timeout, poll_interval, clock=None):
        self.clock = clock or Clock()
        self.case_id = case_id
        self.refund_id = case_id + "-r1"
        self.payment_id = case_id + "-p1"
        self.order_id = case_id + "-o1"
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.started_monotonic = self.clock.monotonic()
        self.result = {
            "case_id": case_id,
            "refund_id": self.refund_id,
            "payment_id": self.payment_id,
            "run_label": run_label,
            "status": "running",
            "outcome": "unknown",
            "commit_outcome": "not_attempted",
            "phase": "preparation",
            "insert_attempted": False,
            "started_at_utc": self.clock.utc(),
            "timeout_seconds": timeout,
            "poll_interval_seconds": poll_interval,
            "environment": {
                "execution": "existing_linux_runner",
                "runner_hostname": socket.gethostname(),
                "source_endpoint": "source-db:5432/app",
                "spark_endpoint": "spark:10000",
                "spark_session": "one warmed connection and cursor",
                "refresh_before_guard_and_each_poll": True,
            },
            "guards": {},
            "t0": None,
            "t1": None,
            "t2": None,
            "request_to_observation_seconds": None,
            "commit_ack_to_observation_seconds": None,
            "polls": [],
            "observation_interval": None,
            "matched_record": None,
            "scope": SCOPE,
        }

    def require(self, condition, code):
        if not condition:
            raise MeasurementFailure(code)

    def history(self, cursor, poll=None):
        """Use exactly the same fresh table/read path for guard and polls."""
        cursor.execute("REFRESH TABLE " + TABLE)
        query_start = point(self.clock)
        if poll is not None:
            poll["query_started"] = query_start
        # CASE_PATTERN permits no SQL quote characters. PG queries below bind
        # parameters separately; this validated literal is for Spark SQL.
        sql = (
            "SELECT topic,partition_id,kafka_offset,payload,kafka_ts,ingested_at "
            "FROM " + TABLE + " WHERE topic='" + TOPIC + "' AND ("
            "get_json_object(payload,'$.before.refund_id')='" + self.refund_id + "' "
            "OR get_json_object(payload,'$.after.refund_id')='" + self.refund_id + "') "
            "ORDER BY partition_id,kafka_offset"
        )
        cursor.execute(sql)
        rows = cursor.fetchall()
        fetched = point(self.clock)  # Immediately after the result is returned.
        if poll is not None:
            poll["query_returned"] = fetched
            poll["record_count"] = len(rows)
            poll["query_seconds"] = round(
                fetched["monotonic_seconds"] - query_start["monotonic_seconds"], 6
            )
        return rows, query_start, fetched

    def source_guards(self, cursor):
        cursor.execute(
            "SELECT p.payment_id,p.order_id,p.amount,p.status,o.order_id,o.status "
            "FROM payments p JOIN orders o ON o.order_id=p.order_id "
            "WHERE p.payment_id=%s",
            (self.payment_id,),
        )
        rows = cursor.fetchall()
        valid = (
            len(rows) == 1
            and rows[0][0] == self.payment_id
            and rows[0][1] == self.order_id
            and amount_is(rows[0][2], 100000)
            and rows[0][3] == "success"
            and rows[0][4] == self.order_id
            and rows[0][5] == "confirmed"
        )
        self.result["guards"]["payment_order_valid"] = valid
        self.require(valid, "expected_prepared_order_and_successful_payment")
        cursor.execute("SELECT refund_id FROM refunds WHERE refund_id=%s", (self.refund_id,))
        absent = not cursor.fetchall()
        self.result["guards"]["source_refund_absent"] = absent
        self.require(absent, "refund_key_already_exists_in_source")

    def matching_record(self, rows):
        self.require(len(rows) == 1, "duplicate_or_unexpected_bronze_history")
        row = rows[0]
        self.require(len(row) == 6, "unexpected_bronze_result_shape")
        try:
            payload = json.loads(row[3])
        except (TypeError, ValueError, UnicodeError):
            raise MeasurementFailure("invalid_bronze_json") from None
        self.require(isinstance(payload, dict), "invalid_bronze_envelope")
        after = payload.get("after")
        self.require(isinstance(after, dict), "missing_create_after_state")
        self.require(
            row[0] == TOPIC
            and isinstance(row[1], int) and not isinstance(row[1], bool) and row[1] >= 0
            and isinstance(row[2], int) and not isinstance(row[2], bool) and row[2] >= 0
            and payload.get("op") == "c"
            and payload.get("before") is None
            and after.get("refund_id") == self.refund_id
            and after.get("payment_id") == self.payment_id
            and amount_is(after.get("amount"), 20000)
            and after.get("status") == "success",
            "bronze_create_does_not_match_inserted_refund",
        )
        return {
            "topic": row[0], "partition_id": row[1], "kafka_offset": row[2],
            "op": "c", "refund_id": self.refund_id, "payment_id": self.payment_id,
            "before_amount": None, "after_amount": 20000,
            "kafka_ts": str(row[4]) if row[4] is not None else None,
            "ingested_at": str(row[5]) if row[5] is not None else None,
        }

    def fail(self, code, error_type=None):
        self.result.update(status="failed", outcome="unknown", error_code=code)
        if error_type is not None:
            self.result["error_type"] = error_type
        self.result["finished_at_utc"] = self.clock.utc()
        return self.result

    def timeout_report(self):
        # Called by the hard watchdog. Never poll, retry, rollback or delete.
        return self.fail("overall_deadline_exceeded")

    def expired(self):
        return self.clock.monotonic() - self.started_monotonic >= self.timeout

    def run(self, source_factory, spark_factory):
        source = spark = None
        try:
            self.result["phase"] = "source_connection_and_guards"
            source = source_factory()
            source.autocommit = False
            source_cursor = source.cursor()
            self.source_guards(source_cursor)
            self.result["phase"] = "spark_warmup_and_guard"
            spark = spark_factory()
            spark_cursor = spark.cursor()
            spark_cursor.execute("SELECT 1")
            spark_cursor.fetchall()
            existing, _, _ = self.history(spark_cursor)
            self.result["guards"]["bronze_refund_history_absent"] = not existing
            self.require(not existing, "refund_key_already_exists_in_bronze")
            if self.expired():
                return self.timeout_report()

            # Connections, session warmup and all read guards precede t0.
            insert_sql = (
                "INSERT INTO refunds(refund_id,payment_id,amount,status) "
                "VALUES (%s,%s,%s,%s)"
            )
            insert_values = (self.refund_id, self.payment_id, 20000, "success")
            self.result["phase"] = "insert"
            self.result["insert_attempted"] = True
            self.result["t0"] = point(self.clock)
            source_cursor.execute(insert_sql, insert_values)
            if self.expired():
                return self.timeout_report()
            self.result["phase"] = "commit"
            # A commit call that fails may already have committed remotely.
            self.result["commit_outcome"] = "unknown"
            source.commit()
            self.result["t1"] = point(self.clock)
            self.result["commit_outcome"] = "acknowledged"
            if self.expired():
                return self.timeout_report()

            self.result["phase"] = "bronze_poll"
            last_negative_start = None
            while True:
                if self.expired():
                    return self.timeout_report()
                poll = {"attempt": len(self.result["polls"]) + 1,
                        "refresh_started": point(self.clock), "query_started": None,
                        "query_returned": None, "record_count": None}
                self.result["polls"].append(poll)
                rows, started, fetched = self.history(spark_cursor, poll)
                if self.expired():
                    return self.timeout_report()
                if rows:
                    matched = self.matching_record(rows)
                    self.result["t2"] = fetched
                    self.result["matched_record"] = matched
                    self.result["request_to_observation_seconds"] = round(
                        fetched["monotonic_seconds"] - self.result["t0"]["monotonic_seconds"], 6
                    )
                    self.result["commit_ack_to_observation_seconds"] = round(
                        fetched["monotonic_seconds"] - self.result["t1"]["monotonic_seconds"], 6
                    )
                    lower = last_negative_start or self.result["t0"]
                    self.result["observation_interval"] = {
                        "lower": lower, "upper": fetched,
                        "lower_basis": "last_negative_query_start" if last_negative_start else "t0_no_negative_query",
                        "width_seconds": round(fetched["monotonic_seconds"] - lower["monotonic_seconds"], 6),
                        "scope": "Client query-observation window, not the actual database or Iceberg commit-time interval.",
                    }
                    self.result.update(status="passed", outcome="observed", phase="observed")
                    break
                last_negative_start = started
                remaining = self.timeout - (self.clock.monotonic() - self.started_monotonic)
                if remaining <= 0:
                    return self.timeout_report()
                self.clock.sleep(min(self.poll_interval, remaining))
        except MeasurementFailure as error:
            self.fail(str(error))
        except Exception as error:
            # Do not echo driver exceptions, DSNs, environment or credentials.
            self.fail("dependency_or_query_error", type(error).__name__)
        finally:
            # Ordinary resource closes only. A hard deadline exits the process
            # without running cleanup or attempting any compensating SQL.
            self.result["phase_before_close"] = self.result["phase"]
            if self.result.get("error_code") != "overall_deadline_exceeded":
                for connection in (spark, source):
                    if connection is not None:
                        try:
                            connection.close()
                        except Exception as error:
                            self.result.setdefault("resource_close_error_types", []).append(type(error).__name__)
        self.result["finished_at_utc"] = self.clock.utc()
        return self.result


class HardDeadline:
    """Stop even a blocking DB/Thrift call; never run cleanup after timeout."""
    def __init__(self, seconds, measurement):
        self.measurement = measurement
        self.lock = threading.Lock()
        self.finished = False
        self.timer = threading.Timer(seconds, self.expire)
        self.timer.daemon = True

    def start(self):
        self.timer.start()

    def expire(self):
        with self.lock:
            if self.finished:
                return
            self.finished = True
            report = self.measurement.timeout_report()
            data = (json.dumps(report, ensure_ascii=False, default=str, indent=2) + "\n").encode("utf-8")
            try:
                os.write(sys.stdout.fileno(), data)
            finally:
                # Closing an uncertain commit connection or a stuck Thrift
                # session must not extend the deadline or trigger new calls.
                os._exit(124)

    def emit(self, result):
        with self.lock:
            self.finished = True
            self.timer.cancel()
            print(json.dumps(result, ensure_ascii=False, default=str, indent=2), flush=True)


def source_connection():
    import psycopg2
    return psycopg2.connect(
        host="source-db", user="source", password=os.environ["SOURCE_PASSWORD"],
        dbname="app", connect_timeout=10,
        options="-c statement_timeout=10000 -c lock_timeout=5000",
    )


def spark_connection():
    from scripts.pipeline import connect
    return connect(wait=False)


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("case_id", help="Prepared unused s01- case with an existing order/payment")
    result.add_argument("--insert-refund", action="store_true", help="Explicitly insert exactly one new 20000 refund")
    result.add_argument("--run-label", choices=("rehearsal", "live"), help="Required for an INSERT; distinguish prepared evidence from a live demonstration")
    result.add_argument("--timeout", type=int, default=180, help="Whole-command deadline, 120 through 300 seconds")
    result.add_argument("--poll-interval", type=float, default=1.0, help="Seconds between successful negative reads, 0.1 through 10")
    return result


def main(argv=None):
    command = parser()
    args = command.parse_args(argv)
    if not re.fullmatch(CASE_PATTERN, args.case_id):
        command.error("Use s01- followed by 3-48 lowercase letters, digits or hyphens")
    if not 120 <= args.timeout <= 300:
        command.error("--timeout must be between 120 and 300 seconds")
    if not 0.1 <= args.poll_interval <= 10:
        command.error("--poll-interval must be between 0.1 and 10 seconds")
    if not args.insert_refund:
        print(json.dumps({
            "status": "not_run", "outcome": "no_insert_requested", "case_id": args.case_id,
            "plan": ["Use the prepared order and successful 100000 payment",
                     "Guard source and Bronze refund history are empty",
                     "Warm one Spark session; refresh before the guard and every poll",
                     "Record t0; INSERT one 20000 refund; explicit commit; record t1",
                     "Poll the same key until the first matching fetch returns t2"],
            "required_write_flags": "--insert-refund --run-label rehearsal|live",
            "scope": "Plan only: no connections, files, DML, retries or service changes.",
        }, ensure_ascii=False, indent=2))
        return 0
    if args.run_label is None:
        command.error("--run-label rehearsal or live is required with --insert-refund")
    measurement = Measurement(args.case_id, args.run_label, args.timeout, args.poll_interval)
    deadline = HardDeadline(args.timeout, measurement)
    deadline.start()
    result = measurement.run(source_connection, spark_connection)
    deadline.emit(result)
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
