"""Run fixed Bronze-shaped fixtures through visible native Spark SQL.

Only lake.lesson04.<run_id>_bronze/refunds are written. Source PostgreSQL,
Kafka, Flink, and the shared Bronze/Silver/Gold are never changed.
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.pipeline import connect  # noqa: E402

TOPIC = "cdc.public.refunds"
FIXTURES = (("c", 100, 10, 20000), ("u", 200, 11, 30000), ("d", 300, 12, 30000))


def payload(op, lsn, amount):
    row = {"refund_id": "refund-r1", "payment_id": "payment-p1", "amount": amount, "status": "success"}
    before = None if op == "c" else dict(row, amount=20000 if op == "u" else amount)
    return json.dumps({"before": before, "after": None if op == "d" else row,
                       "op": op, "source": {"lsn": lsn, "ts_ms": 1792900000000 + lsn}}, separators=(",", ":"))


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


class Lesson:
    def __init__(self, cur, run_id):
        if not re.fullmatch(r"[a-z][a-z0-9_-]{0,39}", run_id):
            raise ValueError("run-id: 영문 소문자로 시작하고 소문자/숫자/_/-만 사용하세요(최대 40자).")
        self.cur = cur
        self.run_id = run_id
        prefix = run_id.replace("-", "_")
        self.names = {"bronze": f"lake.lesson04.{prefix}_bronze", "silver": f"lake.lesson04.{prefix}_refunds",
                      "incoming": f"s04_{prefix}_incoming"}
        self.records = []

    def sql(self, sql, fetch=False):
        self.cur.execute(sql)
        return self.cur.fetchall() if fetch else None

    def template(self, name, **extra):
        text = (Path(__file__).parent / "sql" / name).read_text()
        return text.format(**self.names, **extra)

    def record(self, step, **data):
        result = {"step": step, **data}
        self.records.append(result)
        print(json.dumps(result, ensure_ascii=False, default=str), flush=True)

    def prepare(self):
        self.sql("CREATE NAMESPACE IF NOT EXISTS lake.lesson04")
        self.sql(f"CREATE TABLE IF NOT EXISTS {self.names['bronze']} (payload STRING, topic STRING, partition_id INT, kafka_offset BIGINT, kafka_ts TIMESTAMP, ingested_at TIMESTAMP) USING iceberg")
        self.sql(f"CREATE TABLE IF NOT EXISTS {self.names['silver']} (refund_id STRING, payment_id STRING, amount BIGINT, status STRING, is_deleted BOOLEAN, source_lsn BIGINT, topic STRING, partition_id INT, kafka_offset BIGINT) USING iceberg TBLPROPERTIES ('write.merge.mode'='copy-on-write')")
        n = self.sql(f"SELECT COUNT(*) FROM {self.names['bronze']}", True)[0][0]
        if n == 0:
            for op, lsn, offset, amount in FIXTURES:
                self.append(op, lsn, offset, amount)
        merge_mode = self.sql(f"SHOW TBLPROPERTIES {self.names['silver']} ('write.merge.mode')", True)
        self.record("prepare", tables=self.names, input_kind="fixed Bronze-shaped fixture; not new CDC collection", source_unchanged=True, merge_mode=merge_mode)

    def append(self, op, lsn, offset, amount):
        body = literal(payload(op, lsn, amount))
        self.sql(f"INSERT INTO {self.names['bronze']} VALUES ({body}, '{TOPIC}', 0, {offset}, TIMESTAMP '2026-10-25 10:00:00', CURRENT_TIMESTAMP())")

    def validate(self):
        # Fail explicitly. No silently dropped keys, missing versions, or bad casts.
        rows = self.sql(f"""WITH e AS (
          SELECT *, get_json_object(payload,'$.op') op,
          try_cast(get_json_object(payload,'$.source.lsn') AS BIGINT) lsn,
          CASE WHEN get_json_object(payload,'$.op')='d'
          THEN get_json_object(payload,'$.before') ELSE get_json_object(payload,'$.after') END row_json
          FROM {self.names['bronze']}
        ) SELECT COUNT(*) FROM e WHERE payload IS NULL OR op IS NULL OR op NOT IN ('c','u','d','r')
          OR lsn IS NULL OR lsn < 0 OR get_json_object(row_json,'$.refund_id') IS NULL
          OR trim(get_json_object(row_json,'$.refund_id')) = ''
          OR try_cast(get_json_object(row_json,'$.amount') AS BIGINT) IS NULL
          OR topic IS NULL OR topic <> '{TOPIC}' OR partition_id IS NULL OR partition_id <> 0
          OR kafka_offset IS NULL OR kafka_offset < 0""", True)
        if rows[0][0]:
            raise ValueError(f"입력 계약 위반 {rows[0][0]}행: PK·숫자 LSN·금액·op·토픽·파티션·offset을 확인하세요. MERGE하지 않았습니다.")
        conflicts = self.sql(f"SELECT topic,partition_id,kafka_offset FROM {self.names['bronze']} GROUP BY topic,partition_id,kafka_offset HAVING COUNT(DISTINCT payload)>1", True)
        if conflicts:
            raise ValueError(f"같은 전달 위치에 서로 다른 원문이 있습니다: {conflicts}. 임의로 최신 행을 고르지 않고 중단합니다.")
        self.assert_unique()

    def assert_unique(self):
        rows = self.sql(f"SELECT refund_id,COUNT(*) FROM {self.names['silver']} GROUP BY refund_id HAVING COUNT(*) > 1 OR refund_id IS NULL", True)
        if rows:
            raise AssertionError(f"Silver 업무 키 중복/누락: {rows}")

    def latest(self, upper=9223372036854775807):
        self.validate()
        self.sql(self.template("01-latest.sql", upper_lsn=upper))
        rows = self.sql(f"SELECT * FROM {self.names['incoming']} ORDER BY refund_id", True)
        self.record("latest", upper_lsn=upper, rows=rows,
                    meaning="delete 포함해서 최신 1행 선택; 이 단계에서 삭제 행을 제외하지 않음")
        return rows

    def state(self):
        return self.sql(f"SELECT refund_id,amount,is_deleted,source_lsn,kafka_offset FROM {self.names['silver']} ORDER BY refund_id", True)

    def storage(self):
        return {
            "current_files": self.sql(f"SELECT file_path,record_count FROM {self.names['silver']}.files ORDER BY file_path", True),
            "latest_snapshot": self.sql(f"SELECT snapshot_id,parent_id,operation FROM {self.names['silver']}.snapshots ORDER BY committed_at DESC LIMIT 1", True),
        }

    def merge(self, stage, expected=True):
        upper = {"c": 100, "u": 200, "d": 300, "all": 9223372036854775807}[stage]
        self.latest(upper)
        before_storage = self.storage()
        self.sql(self.template("02-merge.sql"))
        self.assert_unique()
        rows = self.state()
        if expected:
            amount, deleted, lsn = {"c": (20000, False, 100), "u": (30000, False, 200), "d": (30000, True, 300), "all": (30000, True, 300)}[stage]
            assert len(rows) == 1 and rows[0][0:4] == ("refund-r1", amount, deleted, lsn), rows
        self.record("merge", stage=stage, stored_rows=rows,
                    current_rows=self.sql(f"SELECT refund_id,amount FROM {self.names['silver']} WHERE NOT is_deleted", True),
                    before_storage=before_storage, after_storage=self.storage())

    def replay(self):
        # A true delivery duplicate repeats the same transport identity.
        self.append("d", 300, 12, 30000)
        self.merge("all")
        self.record("delivery_duplicate", expectation="Bronze may contain duplicate delivery rows; Silver stays one deleted business key")
        # The old source change is republished at a NEW Kafka offset.
        self.append("u", 200, 13, 30000)
        # Select the OLD batch explicitly. Ranking all inputs would pick d/300
        # and would not exercise the MERGE target's version guard at all.
        incoming = self.latest(200)
        assert len(incoming) == 1 and incoming[0][4:6] == (False, 200) and incoming[0][8] == 13, incoming
        before = self.state()
        assert len(before) == 1 and before[0][0:4] == ("refund-r1", 30000, True, 300), before
        before_storage = self.storage()
        self.sql(self.template("02-merge.sql"))
        after = self.state()
        assert after == before, (before, after)
        self.record("old_update_replay", incoming_lsn=200, incoming_kafka_offset=13,
                    incoming_is_deleted=False, target_lsn=300, target_kafka_offset=12,
                    guard_rejected=True, stored_rows=after,
                    before_storage=before_storage, after_storage=self.storage())
        before = self.state()
        self.merge("all")
        assert self.state() == before
        self.record("same_input_rerun", unchanged=True)

    def verify(self):
        self.validate()
        rows = self.state()
        assert len(rows) == 1 and rows[0][0:4] == ("refund-r1", 30000, True, 300), rows
        current = self.sql(f"SELECT COUNT(*) FROM {self.names['silver']} WHERE NOT is_deleted", True)[0][0]
        assert current == 0
        duplicates = self.sql(f"SELECT topic,partition_id,kafka_offset,COUNT(*) FROM {self.names['bronze']} GROUP BY topic,partition_id,kafka_offset HAVING COUNT(*)>1", True)
        self.record("verify", passed=True, stored_rows=rows, current_business_row_count=current, bronze_delivery_duplicates=duplicates)

    def save(self):
        out = ROOT / ".runtime" / "session04" / self.run_id / "report.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        report = {"run_id": self.run_id, "verified_at": datetime.now(timezone.utc).isoformat(),
                  "scope": "lake.lesson04 only; fixed fixtures, not CDC ingestion or common mart refresh",
                  "tables": self.names, "results": self.records}
        if out.exists():
            previous = json.loads(out.read_text())
            if previous.get("run_id") != self.run_id:
                raise ValueError("기존 보고서의 run-id가 다릅니다. 덮어쓰지 않았습니다.")
            report["results"] = previous.get("results", []) + self.records
        out.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str) + "\n")
        print(f"REPORT: {out.relative_to(ROOT)}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "latest", "merge", "replay", "verify", "all"))
    parser.add_argument("--run-id", required=True, help="새 실습별 고유 ID. 예: s04-myname-01")
    parser.add_argument("--stage", choices=("c", "u", "d", "all"), default="all")
    args = parser.parse_args()
    with connect() as conn:
        lesson = Lesson(conn.cursor(), args.run_id)
        if args.action in ("prepare", "all"):
            lesson.prepare()
        if args.action == "all":
            if lesson.state():
                raise ValueError("이 run-id의 Silver가 이미 존재합니다. all은 새 run-id로 시작하세요. 동일 입력 재실행은 merge --stage all 입니다.")
            lesson.latest()
            for stage in ("c", "u", "d"):
                lesson.merge(stage)
            lesson.replay()
            lesson.verify()
        elif args.action == "latest":
            lesson.latest()
        elif args.action == "merge":
            lesson.merge(args.stage)
        elif args.action == "replay":
            lesson.replay()
        elif args.action == "verify":
            lesson.verify()
        lesson.save()


if __name__ == "__main__":
    main()
