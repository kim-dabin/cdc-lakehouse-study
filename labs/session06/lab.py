"""6회차 고정 입력: 수정·삭제·소급 정정과 증분·재처리를 비교해요.

dbt table 모델은 매번 전체를 다시 써서 snapshot 차이가 업무 변경을 뜻하지 않아요.
그래서 Silver core 앞의 현재 상태 테이블을 본뜬 격리 입력에서 변경을 읽어요.
한 run-id는 한 사람이 순서대로 실행해요. 실험 중 입력 변경·snapshot 만료는 쉬어 주세요.
"""
import argparse
import hashlib
import json
import re
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.pipeline import MODELS, connect  # noqa: E402

INPUTS = tuple(MODELS)
TABLES = (*INPUTS, "gold_order", "gold_daily", "progress", "plan")
EXTRA = {"is_deleted": "BOOLEAN", "source_lsn": "BIGINT",
         "kafka_offset": "BIGINT", "source_ts_ms": "BIGINT"}
COW = "TBLPROPERTIES (" + ", ".join(
    f"'write.{op}.mode'='copy-on-write'" for op in ("merge", "update", "delete")) + ")"
ORDER_COLUMNS = "order_id,order_date,paid,refunded,net_paid"
DAILY_COLUMNS = "order_date,order_count,paid,refunded,net_paid"
SCENARIOS = tuple(f"s{i}" for i in range(1, 7))


def literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def fields(table):
    return {**MODELS[table][1], **EXTRA}


def fixture_row(lsn, **values):
    return {**values, "is_deleted": values.get("is_deleted", False),
            "source_lsn": lsn, "kafka_offset": lsn, "source_ts_ms": 1793491200000 + lsn}


def fixture_state(stage):
    """각 단계가 끝났을 때의 고정 입력이에요."""
    state = {table: [] for table in INPUTS}
    if stage == 0:
        return state
    ordered_at = {1: "2026-11-01 01:00:00", 2: "2026-11-02 01:00:00"}
    if stage >= 2:
        ordered_at[3] = "2026-11-02 03:00:00"
    if stage >= 5:
        ordered_at[1] = "2026-10-28 01:00:00"
    if stage >= 6:
        ordered_at[1] = "2026-10-25 01:00:00"
    for number, timestamp in ordered_at.items():
        lsn = 600 if number == 1 and stage >= 6 else 500 if number == 1 and stage >= 5 else 200 if number == 3 else 100
        state["orders"].append(fixture_row(lsn, order_id=f"o{number}",
                                            ordered_at=timestamp, status="created"))
    state["payments"].append(fixture_row(100, payment_id="p1", order_id="o1",
                                       amount=100000, status="success"))
    if stage >= 2:
        state["payments"].append(fixture_row(200, payment_id="p3", order_id="o3",
                                           amount=50000, status="success"))
    state["refunds"].append(fixture_row(400 if stage >= 4 else 300 if stage >= 3 else 100,
                                      refund_id="r1", payment_id="p1",
                                      amount=30000 if stage >= 3 else 20000,
                                      status="success", is_deleted=stage >= 4))
    return state


def expected(stage):
    """SQL과 별개로 적은 고정 기대값이에요."""
    old_date = "2026-10-25" if stage >= 6 else "2026-10-28" if stage >= 5 else "2026-11-01"
    refund = 0 if stage >= 4 else 30000 if stage >= 3 else 20000
    orders = [["o1", old_date, 100000, refund, 100000 - refund],
              ["o2", "2026-11-02", 0, 0, 0]]
    if stage >= 2:
        orders.append(["o3", "2026-11-02", 50000, 0, 50000])
    daily = [[old_date, 1, 100000, refund, 100000 - refund],
             ["2026-11-02", 2 if stage >= 2 else 1,
              50000 if stage >= 2 else 0, 0, 50000 if stage >= 2 else 0]]
    return {"gold_order": orders, "gold_daily": sorted(daily)}


class InjectedFailure(RuntimeError):
    pass


class Lesson:
    def __init__(self, cur, run_id):
        if not re.fullmatch(r"s06-[a-z0-9_-]{1,36}", run_id):
            raise ValueError("run-id: s06-로 시작하고 소문자/숫자/_/-만 사용하세요(최대 40자).")
        self.cur = cur
        self.run_id = run_id
        prefix = run_id.replace("-", "_")
        self.tables = {name: f"lake.lesson06.{prefix}_{name}" for name in TABLES}
        self.names = dict(self.tables)
        views = ("dim_order", "fct_payment", "fct_refund", "full_order", "full_daily",
                 "affected_keys", "affected_dates", *(f"input_{t}" for t in INPUTS),
                 *(f"changes_{t}" for t in INPUTS))
        self.names.update({name: f"s06_{prefix}_{name}" for name in views})
        self.records = []

    def sql(self, sql, fetch=False):
        self.cur.execute(sql)
        return self.cur.fetchall() if fetch else None

    def template(self, name, **extra):
        text = (Path(__file__).parent / "sql" / name).read_text()
        return text.format(**self.names, **extra)

    def script(self, name):
        for statement in self.template(name).split(";"):
            if statement.strip():
                self.sql(statement)

    def record(self, step, **data):
        result = {"step": step, **data}
        self.records.append(result)
        print(json.dumps(result, ensure_ascii=False, default=str), flush=True)

    def prepare(self):
        self.sql("CREATE NAMESPACE IF NOT EXISTS lake.lesson06")
        existing = self.sql("SHOW TABLES IN lake.lesson06", True)
        wanted = {name.split(".")[-1] for name in self.tables.values()}
        if any(row[1] in wanted for row in existing):
            raise ValueError("이 run-id의 테이블이 있어요. prepare/all은 새 run-id로 시작하세요. 이어 하려면 scenario/incremental/verify/inspect를 쓰세요.")
        for table in INPUTS:
            ddl = ", ".join(f"{col} {kind}" for col, kind in fields(table).items())
            self.sql(f"CREATE TABLE {self.names[table]} ({ddl}) USING iceberg {COW}")
        schemas = {
            "gold_order": "order_id STRING, order_date DATE, paid BIGINT, refunded BIGINT, net_paid BIGINT",
            "gold_daily": "order_date DATE, order_count BIGINT, paid BIGINT, refunded BIGINT, net_paid BIGINT",
            "progress": "input_table STRING, snapshot_id BIGINT",
            "plan": "range_id STRING, ranges_json STRING, keys_json STRING, dates_json STRING, saved_at TIMESTAMP",
        }
        for table, ddl in schemas.items():
            self.sql(f"CREATE TABLE {self.names[table]} ({ddl}) USING iceberg {COW}")
        self.record("prepare", message="준비됐어요. scenario --name s1부터 차례로 실행하세요.",
                    tables={k: self.names[k] for k in (*INPUTS, *schemas)})

    def clean(self):
        # 조회 결과나 prefix 검색으로 삭제 대상을 늘리지 않아요.
        for name in self.tables.values():
            self.sql(f"DROP TABLE IF EXISTS {name} PURGE")
        self.record("clean", run_id=self.run_id, tables=list(self.tables.values()),
                    message=f"이 run-id의 지정 테이블 {len(self.tables)}개만 DROP TABLE PURGE했어요.")

    def snapshots(self):
        result = {}
        for table in INPUTS:
            rows = self.sql(f"SELECT snapshot_id FROM {self.names[table]}.refs WHERE name='main'", True)
            result[table] = int(rows[0][0]) if rows else None
        return result

    def ranges(self):
        rows = self.sql(f"SELECT input_table,snapshot_id FROM {self.names['progress']}", True)
        if len({r[0] for r in rows}) != len(rows) or any(r[0] not in INPUTS for r in rows):
            raise ValueError("progress의 입력 이름·중복을 확인하세요.")
        start = dict(rows)
        end = self.snapshots()
        return {t: {"start": start.get(t), "end": end[t]} for t in INPUTS}

    def calculations(self, ends):
        # 한 번 잡은 끝 snapshot으로 계산해요. 빈 입력에는 버전이 없어요.
        for table, snapshot in ends.items():
            source = self.names[table]
            source += f" VERSION AS OF {int(snapshot)}" if snapshot is not None else " WHERE FALSE"
            self.sql(f"CREATE OR REPLACE TEMP VIEW {self.names['input_' + table]} AS SELECT * FROM {source}")
        self.script("01-core.sql")
        self.script("02-full.sql")

    def changelog(self, table, start, end):
        view = self.names["changes_" + table]
        if start == end or end is None:
            self.sql(f"CREATE OR REPLACE TEMP VIEW {view} AS SELECT *, CAST(NULL AS STRING) _change_type FROM {self.names[table]} WHERE FALSE")
            return view
        options = [literal("end-snapshot-id"), literal(str(int(end)))]
        if start is not None:
            options += [literal("start-snapshot-id"), literal(str(int(start)))]
        # 임시 뷰가 없으면 JDBC 카탈로그의 미지원 뷰로 해석되므로 DROP하지 않아요.
        # start는 제외, end는 포함해요. COW의 그대로 옮겨진 행은 기본으로 빠져요.
        self.sql(f"""CALL lake.system.create_changelog_view(
          table => {literal(self.names[table].removeprefix('lake.'))},
          options => map({','.join(options)}), changelog_view => {literal(view)},
          identifier_columns => array({literal(MODELS[table][0])}), compute_updates => true)""")
        return view

    def crosscheck(self, table, start, end, changes):
        pk = MODELS[table][0]
        target = self.names[table]
        before = f"SELECT * FROM {target} VERSION AS OF {int(start)}" if start is not None else f"SELECT * FROM {target} WHERE FALSE"
        after = f"SELECT * FROM {target} VERSION AS OF {int(end)}" if end is not None else f"SELECT * FROM {target} WHERE FALSE"
        equal = " AND ".join(f"(b.{col} <=> a.{col})" for col in fields(table))
        diff = self.sql(f"""WITH b AS ({before}), a AS ({after})
          SELECT coalesce(b.{pk},a.{pk}) changed_key FROM b FULL OUTER JOIN a ON b.{pk}=a.{pk}
          WHERE b.{pk} IS NULL OR a.{pk} IS NULL OR NOT ({equal})""", True)
        logged = self.sql(f"SELECT DISTINCT {pk} FROM {changes}", True)
        diff_keys, log_keys = {r[0] for r in diff}, {r[0] for r in logged}
        # snapshot ID는 시간순 숫자가 아니므로 범위 안의 ID를 ancestry에서 골라요.
        # _change_ordinal은 .changes 스캔 전체 기준이므로 snapshot ID로 제한해요.
        raw = []
        if start != end and end is not None:
            history = self.sql(
                f"SELECT snapshot_id, made_current_at, is_current_ancestor FROM {target}.history", True)
            lineage = sorted(history, key=lambda row: row[1])
            first = max((i for i, row in enumerate(lineage) if row[0] == start), default=-1)
            last = max((i for i, row in enumerate(lineage) if row[0] == end), default=-1)
            snapshot_ids = {row[0] for i, row in enumerate(lineage)
                            if row[2] and first < i <= last}
            # Spark 3.5 / Iceberg 1.10의 메타데이터 필터 오류를 피해 단순 조회 후 걸러요.
            # Spark 3.5.6 / Iceberg 1.10.0 실측: ordinal 없이 snapshot ID를 고르면 Integer → Long ClassCastException이 나요.
            rows = self.sql(
                f"SELECT {pk}, _change_type, _change_ordinal, _commit_snapshot_id FROM {target}.changes", True)
            raw = [row for row in rows if row[3] in snapshot_ids]
        logged_rows = self.sql(f"SELECT {pk} FROM {changes}", True)
        raw_keys = {r[0] for r in raw}
        self.record("carryover", table=table, start=start, end=end,
                    raw_rows=len(raw), raw_keys=sorted(raw_keys),
                    changelog_rows=len(logged_rows), changelog_keys=sorted(log_keys),
                    carryover_keys=sorted(raw_keys - log_keys))
        match = diff_keys == log_keys
        self.record("crosscheck", table=table, start=start, end=end,
                    time_travel_keys=sorted(diff_keys), changelog_keys=sorted(log_keys), match=match)
        if not match:
            raise AssertionError("changelog와 시점 비교의 변경 키가 달라요. Gold에는 쓰지 않았어요.")

    def value_view(self, name, column, kind, values):
        rows = [f"SELECT CAST({literal(value)} AS {kind}) {column}" for value in values]
        query = " UNION ALL ".join(rows) if rows else f"SELECT CAST(NULL AS {kind}) {column} WHERE FALSE"
        self.sql(f"CREATE OR REPLACE TEMP VIEW {self.names[name]} AS {query}")

    def plan(self, ranges, naive_dates=False):
        serialized = json.dumps(ranges, sort_keys=True, separators=(",", ":"))
        range_id = hashlib.sha256(serialized.encode()).hexdigest()
        if not naive_dates:
            saved = self.sql(f"SELECT ranges_json,keys_json,dates_json FROM {self.names['plan']} WHERE range_id={literal(range_id)}", True)
            if saved:
                if len(saved) != 1 or saved[0][0] != serialized:
                    raise ValueError("저장한 계획의 범위·중복을 확인하세요.")
                keys, dates = json.loads(saved[0][1]), json.loads(saved[0][2])
                self.record("plan", reused=True, range_id=range_id, ranges=ranges, keys=keys, dates=dates)
                return keys, dates
        for table, bounds in ranges.items():
            view = self.changelog(table, **bounds)
            self.crosscheck(table, changes=view, **bounds)
        self.sql(f"""CREATE OR REPLACE TEMP VIEW {self.names['affected_keys']} AS
          SELECT order_id FROM {self.names['changes_orders']}
          UNION SELECT order_id FROM {self.names['changes_payments']}
          UNION SELECT p.order_id FROM {self.names['changes_refunds']} r
          JOIN {self.names['input_payments']} p ON r.payment_id=p.payment_id""")
        keys = sorted({r[0] for r in self.sql(f"SELECT order_id FROM {self.names['affected_keys']} WHERE order_id IS NOT NULL", True)})
        self.value_view("affected_keys", "order_id", "STRING", keys)
        date_query = f"""SELECT g.order_date FROM {self.names['gold_order']} g
          JOIN {self.names['affected_keys']} k ON g.order_id=k.order_id
          UNION SELECT n.order_date FROM {self.names['full_order']} n
          JOIN {self.names['affected_keys']} k ON n.order_id=k.order_id"""
        if not naive_dates:
            date_query += f""" UNION SELECT to_date(from_utc_timestamp(ordered_at,'Asia/Seoul')) order_date
              FROM {self.names['changes_orders']} WHERE _change_type IN ('UPDATE_BEFORE','DELETE')"""
        dates = sorted({str(r[0]) if r[0] is not None else None for r in self.sql(date_query, True)}, key=lambda v: v or "")
        if not naive_dates:
            self.sql(f"""INSERT INTO {self.names['plan']} VALUES (
              {literal(range_id)}, {literal(serialized)},
              {literal(json.dumps(keys))}, {literal(json.dumps(dates))}, CURRENT_TIMESTAMP())""")
        self.record("plan", reused=False, persisted=not naive_dates, range_id=range_id,
                    ranges=ranges, keys=keys, dates=dates, naive_dates=naive_dates)
        return keys, dates

    def incremental(self, fail_after=None, naive_dates=False):
        ranges = self.ranges()
        self.calculations({t: r["end"] for t, r in ranges.items()})
        keys, dates = self.plan(ranges, naive_dates)
        # 저장한 키·날짜를 뷰에 고정하고 Gold를 갱신해요.
        self.value_view("affected_keys", "order_id", "STRING", keys)
        self.value_view("affected_dates", "order_date", "DATE", dates)
        self.sql(self.template("03-order-merge.sql"))
        if fail_after == "order":
            self.record("injected_failure", message="주문별 갱신 뒤 멈췄어요. progress는 진행하지 않았어요.", ranges=ranges)
            raise InjectedFailure("주문별 갱신 뒤 일부러 멈췄어요. incremental로 같은 범위를 다시 실행하세요.")
        self.sql(self.template("04-daily-merge.sql"))
        source = " UNION ALL ".join(
            f"SELECT {literal(t)} input_table, CAST({literal(r['end'])} AS BIGINT) snapshot_id"
            for t, r in ranges.items())
        # 두 Gold 갱신이 모두 끝나야 성공 위치를 한 번에 저장해요.
        self.sql(f"""MERGE INTO {self.names['progress']} t USING ({source}) s ON t.input_table=s.input_table
          WHEN MATCHED THEN UPDATE SET t.snapshot_id=s.snapshot_id
          WHEN NOT MATCHED THEN INSERT (input_table,snapshot_id) VALUES (s.input_table,s.snapshot_id)""")
        self.record("incremental", message="주문별·일별 갱신 후 성공 snapshot을 저장했어요.",
                    ranges=ranges, keys=keys, dates=dates, naive_dates=naive_dates)
        return ranges

    def difference(self, actual, reference, columns):
        left = self.sql(f"SELECT COUNT(*) FROM (SELECT {columns} FROM {actual} EXCEPT SELECT {columns} FROM {reference}) d", True)[0][0]
        right = self.sql(f"SELECT COUNT(*) FROM (SELECT {columns} FROM {reference} EXCEPT SELECT {columns} FROM {actual}) d", True)[0][0]
        return {"actual_minus_full": left, "full_minus_actual": right, "match": left == right == 0}

    def verify(self):
        self.calculations(self.snapshots())
        order = self.difference(self.names["gold_order"], self.names["full_order"], ORDER_COLUMNS)
        daily = self.difference(self.names["gold_daily"], self.names["full_daily"], DAILY_COLUMNS)
        match = order["match"] and daily["match"]
        self.record("verify", gold_order=order, gold_daily=daily, match=match)
        return match

    def upsert(self, table, rows):
        if not rows:
            return
        columns = fields(table)
        pk = MODELS[table][0]
        source = " UNION ALL ".join(
            "SELECT " + ", ".join(f"CAST({literal(row[c])} AS {kind}) {c}" for c, kind in columns.items())
            for row in rows)
        assigns = ", ".join(f"t.{c}=s.{c}" for c in columns)
        self.sql(f"""MERGE INTO {self.names[table]} t USING (
          SELECT /*+ REPARTITION(1) */ * FROM ({source}) batch
        ) s ON t.{pk}=s.{pk}
          WHEN MATCHED AND (s.source_lsn>t.source_lsn OR
            (s.source_lsn=t.source_lsn AND s.kafka_offset>t.kafka_offset)) THEN UPDATE SET {assigns}
          WHEN NOT MATCHED THEN INSERT ({','.join(columns)}) VALUES ({','.join('s.' + c for c in columns)})""")

    def assert_fixture(self, stage):
        for table, rows in fixture_state(stage).items():
            columns = list(fields(table))
            actual = self.sql(f"SELECT {','.join(columns)} FROM {self.names[table]} ORDER BY {MODELS[table][0]}", True)
            normalized = [[v.isoformat(sep=" ") if isinstance(v, datetime) else v for v in r] for r in actual]
            wanted = [[r[c] for c in columns] for r in rows]
            if normalized != wanted:
                raise ValueError(f"{table} 입력이 s{stage}의 상태와 달라요. 새 run-id로 all을 실행하거나 이전 단계를 확인하세요.")

    def check_expected(self, stage):
        wanted = expected(stage)
        observed = {}
        for table, columns in (("gold_order", ORDER_COLUMNS), ("gold_daily", DAILY_COLUMNS)):
            rows = self.sql(f"SELECT {columns} FROM {self.names[table]} ORDER BY {columns.split(',')[0]}", True)
            observed[table] = [[v.isoformat() if isinstance(v, date) else v for v in r] for r in rows]
        match = observed == wanted
        self.record("expected", scenario=f"s{stage}", expected=wanted, actual=observed, match=match)
        return match

    def scenario(self, name, naive_dates=False):
        if name == "all":
            for stage in SCENARIOS:
                self.scenario(stage, naive_dates)
            return
        stage = int(name[1])
        self.assert_fixture(stage - 1)
        if not self.verify():
            raise AssertionError("이전 단계부터 전체 계산과 달라요. 먼저 incremental로 복구하세요.")
        before = fixture_state(stage - 1)
        for table, rows in fixture_state(stage).items():
            self.upsert(table, [row for row in rows if row not in before[table]])
        self.record("scenario", name=name, expected=expected(stage))
        if stage == 6:
            bounds = self.ranges()
            try:
                self.incremental(fail_after="order", naive_dates=naive_dates)
            except InjectedFailure:
                self.record("retry", message="같은 범위를 다시 실행해요.",
                            progress_unchanged=self.ranges() == bounds)
                if self.ranges() != bounds:
                    raise AssertionError("실패했는데 진행 위치나 입력 범위가 바뀌었어요.")
            else:
                raise AssertionError("실패 지점에 도달하지 않았어요.")
        self.incremental(naive_dates=naive_dates)
        match = self.verify()
        fixed_match = self.check_expected(stage)
        expected_match = not (stage == 6 and naive_dates)
        self.record("scenario_result", name=name, match=match, fixed_expected_match=fixed_match,
                    expected_match=expected_match, passed=match == fixed_match == expected_match)
        if not (match == fixed_match == expected_match):
            raise AssertionError("시나리오 기대와 달라요. verify·expected 기록을 확인하세요.")

    def inspect(self):
        ranges = self.ranges()
        saved = self.sql(f"SELECT ranges_json FROM {self.names['plan']} ORDER BY saved_at DESC LIMIT 1", True)
        if saved:
            ranges = json.loads(saved[0][0])
        for table in TABLES:
            snapshots = self.sql(f"SELECT snapshot_id,parent_id,operation FROM {self.names[table]}.snapshots ORDER BY committed_at DESC LIMIT 5", True)
            rows = self.sql(f"SELECT * FROM {self.names[table]} LIMIT 5", True)
            self.record("inspect", table=table, snapshots=snapshots, rows=rows)
        for table, bounds in ranges.items():
            view = self.changelog(table, **bounds)
            rows = self.sql(f"SELECT * FROM {view} LIMIT 8", True)
            self.record("inspect_changelog", table=table, **bounds, rows=rows,
                        message="저장된 최근 계획 범위예요." if saved else "마지막 성공 이후 범위예요. 성공 직후에는 비어 있어요.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "scenario", "incremental", "verify", "inspect", "all", "clean"))
    parser.add_argument("--run-id", required=True, help="실습별 고유 ID. 예: s06-myname-01")
    parser.add_argument("--name", choices=(*SCENARIOS, "all"), default="all", help="scenario 단계. s1부터 순서대로 실행해요.")
    parser.add_argument("--fail-after", choices=("order",), help="incremental의 주문별 갱신 뒤에 멈춰요.")
    parser.add_argument("--naive-dates", action="store_true", help="날짜 계획 없이 재시도해 s6의 누락을 비교해요.")
    args = parser.parse_args()
    if args.fail_after and args.action != "incremental":
        parser.error("--fail-after는 incremental에서 쓰세요. s6는 스스로 실패·재시도해요.")
    # 연결 전에 ID부터 확인해요.
    Lesson(None, args.run_id)
    with connect() as conn:
        lesson = Lesson(conn.cursor(), args.run_id)
        lesson.sql("SET spark.sql.session.timeZone=UTC")
        if args.action in ("prepare", "all"):
            lesson.prepare()
        if args.action == "all":
            lesson.scenario("all", args.naive_dates)
        elif args.action == "scenario":
            lesson.scenario(args.name, args.naive_dates)
        elif args.action == "incremental":
            lesson.incremental(args.fail_after, args.naive_dates)
        elif args.action == "verify":
            if not lesson.verify():
                raise AssertionError("전체 계산과 달라요. verify 기록을 확인하세요.")
        elif args.action == "inspect":
            lesson.inspect()
        elif args.action == "clean":
            lesson.clean()


if __name__ == "__main__":
    main()
