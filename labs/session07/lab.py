"""7회차 고정 실험: 작은 커밋을 쌓고 파일·snapshot·메타데이터를 정리해요.

스트리밍이 checkpoint마다 작은 커밋을 쌓는 상황을 INSERT로 흉내 내요.
빈 커밋 자체를 만드는 실험은 아니에요. 한 run-id는 한 사람이 순서대로 실행해요.
실험 중에는 이 run-id 테이블에 동시에 쓰는 작업이 없어야 해요.
"""
import argparse
from datetime import timedelta
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.pipeline import connect  # noqa: E402

TABLES = ("plain", "tuned", "inc")
PAIRS = TABLES[:2]
STEPS = ("prepare", "grow", "compact", "expire", "orphans", "manifests",
         "progress-trap", "verify")
SCHEMA = "event_id STRING, order_id STRING, amount BIGINT, event_ts TIMESTAMP"
BASE_ROWS = "lesson07.grow.rows"
BASE_SUM = "lesson07.grow.checksum"


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def orphan_counts(rows):
    """프로시저의 orphan_file_location을 파일 종류별로 세어요."""
    counts = {"metadata.json": 0, "avro": 0, "parquet": 0, "other": 0}
    for row in rows:
        path = str(row[0]).lower()
        kind = next((suffix for suffix in ("metadata.json", "avro", "parquet")
                     if path.endswith("." + suffix)), "other")
        counts[kind] += 1
    return counts


def metadata_version(file):
    """숫자-식별자.metadata.json 형식의 파일명에서 버전을 읽어요. 다른 형식은 None이에요."""
    match = re.fullmatch(r"([0-9]+)-[^/]+\.metadata\.json", str(file).rsplit("/", 1)[-1])
    return int(match[1]) if match else None


def expected(commits):
    return {"rows": commits, "checksum": 100 * commits * (commits + 1) // 2}


class Lesson:
    def __init__(self, cur, run_id):
        if not re.fullmatch(r"s07-[a-z0-9_-]{1,36}", run_id):
            raise ValueError("run-id: s07-로 시작하고 소문자/숫자/_/-만 사용하세요(최대 40자).")
        self.cur = cur
        self.run_id = run_id
        prefix = run_id.replace("-", "_")
        self.tables = {name: f"lake.lesson07.{prefix}_{name}" for name in TABLES}
        self.records = []
        self.measurements = []

    def sql(self, sql, fetch=False):
        self.cur.execute(sql)
        return self.cur.fetchall() if fetch else None

    def record(self, step, **data):
        result = {"step": step, "run_id": self.run_id, **data}
        self.records.append(result)
        print(json.dumps(result, ensure_ascii=False, default=str), flush=True)

    def now(self):
        # SQL 서버의 시각을 써서 파일 시각과 클라이언트 시각의 차이를 피해요.
        return self.sql("SELECT current_timestamp()", True)[0][0]

    def procedure(self, name, table, arguments=""):
        # 외부 테이블 이름은 받지 않고 검증된 run-id의 세 테이블만 써요.
        target = self.tables[table].removeprefix("lake.")
        return self.sql(f"CALL lake.system.{name}(table => {literal(target)}{arguments})", True)

    def procedure_result(self, step, name, table, arguments=""):
        rows = self.procedure(name, table, arguments)
        # 결과 컬럼은 Iceberg 1.10 프로시저의 JDBC description 그대로 기록해요.
        columns = [column[0] for column in self.cur.description]
        self.record(step, table=table, procedure=name, columns=columns,
                    results=[dict(zip(columns, row)) for row in rows])
        return rows

    def orphan_files(self, table, cutoff, dry_run=True):
        # S3FileIO 환경에서 Hadoop s3 FileSystem 없이 목록을 읽기 위해 prefix_listing을 사용해요.
        return self.procedure("remove_orphan_files", table,
                              f", older_than => TIMESTAMP {literal(cutoff)}"
                              ", prefix_listing => true"
                              + (", dry_run => true" if dry_run else ""))

    def prepare(self):
        self.sql("CREATE NAMESPACE IF NOT EXISTS lake.lesson07")
        existing = self.sql("SHOW TABLES IN lake.lesson07", True)
        wanted = {name.rsplit(".", 1)[1] for name in self.tables.values()}
        if any(row[1] in wanted for row in existing):
            raise ValueError("이 run-id의 테이블이 있어요. prepare/all은 새 run-id로 시작하세요. 이어 하려면 각 단계의 action을 쓰세요.")
        for name, table in self.tables.items():
            props = (" TBLPROPERTIES ('write.metadata.delete-after-commit.enabled'='true', "
                     "'write.metadata.previous-versions-max'='10')" if name == "tuned" else "")
            self.sql(f"CREATE TABLE {table} ({SCHEMA}) USING iceberg{props}")
        self.record("prepare", tables=self.tables, message="준비됐어요. grow부터 차례로 실행하세요.")
        self.measure("prepare")

    def totals(self, table):
        row = self.sql(f"SELECT count(*), coalesce(sum(amount),0) FROM {self.tables[table]}", True)[0]
        return {"rows": int(row[0]), "checksum": int(row[1])}

    def baseline(self, table):
        props = dict(self.sql(f"SHOW TBLPROPERTIES {self.tables[table]}", True))
        if BASE_ROWS not in props or BASE_SUM not in props:
            return None
        return {"rows": int(props[BASE_ROWS]), "checksum": int(props[BASE_SUM])}

    def insert(self, table, number):
        self.sql(f"INSERT INTO {self.tables[table]} VALUES "
                 f"('e{number:04d}', 'o{number:04d}', {number * 100}, "
                 f"TIMESTAMP '2026-01-01 00:00:00' + INTERVAL {number} SECONDS)")

    def grow(self, commits=120):
        if isinstance(commits, bool) or not isinstance(commits, int) or commits < 1:
            raise ValueError("commits는 1 이상의 정수로 지정하세요.")
        # 둘 다 먼저 확인해 중복 grow나 한쪽만 이어 쓰는 일을 막아요.
        if any(self.totals(t)["rows"] != 0 or self.baseline(t) is not None for t in PAIRS):
            raise ValueError("grow는 비어 있는 새 실험에서 한 번만 실행하세요.")
        # N이 100보다 커야 기본 추적 한도 밖으로 밀려난 metadata.json이 생겨요.
        for number in range(1, commits + 1):
            for table in PAIRS:
                self.insert(table, number)
        wanted = expected(commits)
        for table in PAIRS:
            actual = self.totals(table)
            if actual != wanted:
                raise AssertionError(f"{table}의 grow 결과가 고정 기대값과 달라요: {actual}")
            # 별도 action도 같은 기준과 비교하도록 각 테이블 속성에 보관해요.
            # 이 ALTER는 snapshot 없이 metadata.json을 한 번 더 커밋해요.
            self.sql(f"ALTER TABLE {self.tables[table]} SET TBLPROPERTIES "
                     f"('{BASE_ROWS}'='{wanted['rows']}', '{BASE_SUM}'='{wanted['checksum']}')")
        self.record("grow", commits=commits, expected=wanted,
                    message="행마다 한 번씩 커밋했어요. 기준값 저장으로 메타데이터 커밋이 한 번 더 생겨요.")
        self.measure("grow")

    def compact(self):
        for table in PAIRS:
            self.procedure_result("compact", "rewrite_data_files", table,
                                  ", options => map('min-input-files','2')")
        self.measure("compact")

    def expire_table(self, table, cutoff, step):
        return self.procedure_result(step, "expire_snapshots", table,
                                     f", older_than => TIMESTAMP {literal(cutoff)}, retain_last => 1")

    def expire(self):
        cutoff = self.now()
        for table in PAIRS:
            self.expire_table(table, cutoff, "expire")
        self.measure("expire")

    def orphans(self):
        """24시간 5분보다 오래된 후보만 조회하고 지워요. 새 실험에서는 대개 0개예요.

        Iceberg 1.10은 dry_run도 24시간보다 최근인 기준을 거절해요.
        동시에 쓰는 작업의 아직 커밋하지 않은 파일을 지우지 않도록 보호해요.
        """
        cutoff = self.now() - timedelta(hours=24, minutes=5)
        message = ("서버 시각에서 24시간 5분을 뺀 기준보다 오래된 파일만 대상이에요. "
                   "방금 만든 실험 테이블에서는 대개 0개예요. "
                   "24시간 안전장치는 동시에 쓰는 작업의 아직 커밋하지 않은 파일을 지우지 않게 해요.")
        for table in PAIRS:
            candidates = self.orphan_files(table, cutoff)
            self.record("orphans", table=table, older_than=cutoff, dry_run=True,
                        orphan_file_location=[row[0] for row in candidates],
                        counts=orphan_counts(candidates), message=message)
            # 목록을 먼저 출력한 뒤 같은 시각 기준으로 지워요.
            deleted = self.orphan_files(table, cutoff, dry_run=False)
            self.record("orphans", table=table, older_than=cutoff, dry_run=False,
                        orphan_file_location=[row[0] for row in deleted],
                        counts=orphan_counts(deleted), message=message)
        self.measure("orphans")

    def manifests(self):
        for table in PAIRS:
            self.procedure_result("manifests", "rewrite_manifests", table)
        self.measure("manifests")

    def changelog(self, start, end):
        view = f"s07_{self.run_id.replace('-', '_')}_changes_inc"
        # 6회차처럼 임시 뷰를 DROP하지 않아요. start는 제외, end는 포함해요.
        self.procedure("create_changelog_view", "inc",
                       f", options => map('end-snapshot-id',{literal(int(end))},"
                       f"'start-snapshot-id',{literal(int(start))}), changelog_view => {literal(view)},"
                       " identifier_columns => array('event_id'), compute_updates => true")
        # 뷰 생성만 성공하고 실제 파일 조회가 실패하는 경우도 검사해요.
        return self.sql(f"SELECT count(*) FROM {view}", True)[0][0]

    def check_changelog(self, label, start, end, should_succeed):
        error, rows = None, None
        try:
            rows = self.changelog(start, end)
        except Exception as exc:
            error = str(exc)[:300]
        succeeded = error is None
        passed = succeeded == should_succeed
        self.record("progress-trap", cleanup=label, progress=start, end=end,
                    succeeded=succeeded, expected_success=should_succeed,
                    passed=passed, rows=rows, error=error)
        if not passed:
            raise AssertionError("진행 위치 실험의 기대와 달라요. progress-trap 기록을 확인하세요.")

    def progress_trap(self):
        table = self.tables["inc"]
        if self.totals("inc")["rows"] or self.sql(f"SELECT snapshot_id FROM {table}.snapshots", True):
            raise ValueError("progress-trap은 비어 있는 inc에서 한 번만 실행하세요.")
        for number in range(1, 4):
            self.insert("inc", number)
        history = self.sql(f"SELECT snapshot_id, committed_at FROM {table}.snapshots ORDER BY committed_at", True)
        progress, committed_at = history[1]
        for number in range(4, 7):
            self.insert("inc", number)
        # snapshot ID는 시간순 숫자가 아니므로 main ref에서 끝 위치를 잡아요.
        end = self.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name='main'", True)[0][0]
        self.record("progress-trap", progress=progress, committed_at=committed_at, end=end,
                    message="두 번째 snapshot을 마지막 성공 위치로 기억했어요.")
        self.expire_table("inc", committed_at, "progress-trap-safe-expire")
        self.check_changelog("safe", progress, end, True)
        self.measure("progress-trap-safe")
        self.expire_table("inc", self.now(), "progress-trap-unsafe-expire")
        self.check_changelog("unsafe", progress, end, False)
        self.measure("progress-trap")

    def measure(self, after="measure", fail_on_mismatch=True):
        result = {}
        for name, table in self.tables.items():
            # all_*는 snapshot 간 같은 파일을 반복하므로 경로의 고유 개수를 세요.
            counts = {key: int(self.sql(f"SELECT count({column}) FROM {table}.{suffix}", True)[0][0])
                      for key, suffix, column in (
                          ("snapshots", "snapshots", "*"),
                          ("metadata_log", "metadata_log_entries", "*"),
                          ("all_data_files_distinct", "all_data_files", "DISTINCT file_path"),
                          ("manifests", "manifests", "*"),
                          ("all_manifests_distinct", "all_manifests", "DISTINCT path"))}
            files = self.sql(f"SELECT count(*), coalesce(sum(file_size_in_bytes),0) FROM {table}.files", True)[0]
            latest = self.sql(f"SELECT file FROM {table}.metadata_log_entries "
                              "ORDER BY timestamp DESC LIMIT 1", True)
            version = metadata_version(latest[0][0]) if latest else None
            written = version + 1 if version is not None else None
            actual = {**counts, "data_files": int(files[0]), "data_bytes": int(files[1]),
                      "metadata_version": version,
                      "metadata_json_written_est": written,
                      "metadata_untracked_est": written - counts["metadata_log"] if written is not None else None,
                      "message": ("metadata_json_written_est는 버전 + 1로 추정한 누적 작성 수예요. "
                                  "metadata_untracked_est는 추적 목록 밖으로 밀려난 수의 추정값이에요. "
                                  "plain은 저장소에 남고, tuned는 delete-after-commit으로 커밋 때 지워져요. "
                                  "현재 저장소 파일 수가 아니며, 파일명 형식이 다르면 추정값은 null이에요."),
                      **self.totals(name)}
            baseline = self.baseline(name) if name in PAIRS else None
            actual["baseline"] = baseline
            actual["match"] = (all(actual[k] == v for k, v in baseline.items())
                               if baseline is not None else None)
            result[name] = actual
            self.record("measure", after=after, table=name, **actual)
        self.measurements.append({"after": after, "tables": result})
        if fail_on_mismatch and any(result[t]["match"] is False for t in PAIRS):
            raise AssertionError("유지보수 뒤 행 수·합계가 grow 기준값과 달라요. measure 기록을 확인하세요.")
        return result

    def verify(self):
        current = self.measure("verify", fail_on_mismatch=False)
        checks = []
        for measurement in self.measurements:
            for table in PAIRS:
                values = measurement["tables"][table]
                # prepare는 grow 전이므로 기준값이 아직 없어요.
                if values["baseline"] is not None:
                    checks.append(values["match"] is True)
        match = bool(checks) and all(checks) and all(current[t]["baseline"] is not None for t in PAIRS)
        self.record("verify", match=match, checked=len(checks),
                    message="grow 기준값과 각 단계의 행 수·합계를 비교했어요.")
        return match

    def all(self, commits=120, stop_after=None):
        if stop_after is not None and stop_after not in STEPS:
            raise ValueError("stop-after는 실험 단계 이름으로 지정하세요.")
        if isinstance(commits, bool) or not isinstance(commits, int) or commits < 1:
            raise ValueError("commits는 1 이상의 정수로 지정하세요.")
        for step in STEPS:
            action = getattr(self, step.replace("-", "_"))
            result = action(commits) if step == "grow" else action()
            if step == "verify" and not result:
                raise AssertionError("유지보수 뒤 결과가 달라요. verify 기록을 확인하세요.")
            if step == stop_after:
                break

    def clean(self):
        # 조회 결과나 prefix 검색으로 삭제 대상을 늘리지 않아요.
        for table in self.tables.values():
            self.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        self.record("clean", tables=list(self.tables.values()),
                    message="이 run-id의 지정 테이블 3개만 DROP TABLE PURGE했어요.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=(*STEPS, "measure", "all", "clean"))
    parser.add_argument("--run-id", required=True, help="실습별 고유 ID. 예: s07-myname-01")
    parser.add_argument("--commits", type=int, default=120, help="grow의 커밋 횟수예요. 기본 120, 메타데이터 비교는 100 초과로 실행해요.")
    parser.add_argument("--stop-after", choices=STEPS, help="all을 지정 단계 뒤에 멈춰요.")
    args = parser.parse_args()
    if args.stop_after and args.action != "all":
        parser.error("--stop-after는 all에서 쓰세요.")
    if args.commits < 1:
        parser.error("--commits는 1 이상으로 지정하세요.")
    # 연결 전에 ID부터 확인해요.
    Lesson(None, args.run_id)
    with connect() as conn:
        lesson = Lesson(conn.cursor(), args.run_id)
        lesson.sql("SET spark.sql.session.timeZone=UTC")
        if args.action == "all":
            lesson.all(args.commits, args.stop_after)
        elif args.action == "grow":
            lesson.grow(args.commits)
        else:
            result = getattr(lesson, args.action.replace("-", "_"))()
            if args.action == "verify" and not result:
                raise AssertionError("grow 기준값이 없거나 결과가 달라요. verify 기록을 확인하세요.")


if __name__ == "__main__":
    main()
