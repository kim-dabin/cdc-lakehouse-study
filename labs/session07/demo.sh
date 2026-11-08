#!/usr/bin/env bash
# 수업 전에 고정 run-id를 준비해요. stdout은 lab.py의 JSON 줄이에요.
set -euo pipefail
cd "$(dirname "$0")/../.."

fresh=0
log_path=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --fresh) fresh=1; shift ;;
    --log)
      if [[ $# -lt 2 || -z "$2" ]]; then
        echo "--log 뒤에 JSONL 파일 경로를 지정하세요." >&2
        exit 2
      fi
      log_path="$2"; shift 2 ;;
    -h|--help)
      echo "사용법: ./labs/session07/demo.sh [--fresh] [--log JSONL파일]"
      echo "기존 데모가 있으면 실행 전에 멈춰요. --fresh는 지정 데모 테이블만 지우고 다시 만들어요."
      echo "--log는 저장소 기준 상대 경로 또는 절대 경로에 JSON 줄을 그대로 덧붙여요."
      exit 0 ;;
    *) echo "알 수 없는 인자: $1 (--help로 사용법 확인)" >&2; exit 2 ;;
  esac
done

run_demo() {
  ./scripts/lab.sh exec -T runner python - "$fresh" <<'PY'
import sys

from labs.session07.lab import Lesson, connect

demos = [("s07-demo-grow", "grow", 120),
         ("s07-demo-compact", "compact", 120),
         ("s07-demo-expire", "expire", 120),
         ("s07-demo-clean", "manifests", 120),
         ("s07-demo-trap", "progress-trap", 5)]
with connect() as conn:
    cur = conn.cursor()
    cur.execute("SET spark.sql.session.timeZone=UTC")
    lessons = [(Lesson(cur, run_id), stop, commits) for run_id, stop, commits in demos]
    # 다섯 run 전체를 먼저 확인해 뒤쪽 충돌로 앞쪽만 준비되는 일을 막아요.
    cur.execute("CREATE NAMESPACE IF NOT EXISTS lake.lesson07")
    cur.execute("SHOW TABLES IN lake.lesson07")
    existing = {row[1] for row in cur.fetchall()}
    collisions = [lesson.run_id for lesson, _, _ in lessons
                  if any(name.rsplit(".", 1)[1] in existing for name in lesson.tables.values())]
    if collisions and sys.argv[1] != "1":
        raise SystemExit("이미 있는 데모 run-id: " + ", ".join(collisions)
                         + ". 새로 준비하려면 demo.sh --fresh를 쓰세요.")
    for lesson, stop, commits in lessons:
        if sys.argv[1] == "1":
            lesson.clean()
        lesson.all(commits=commits, stop_after=stop)
PY
}

if [[ -n "$log_path" ]]; then
  mkdir -p "$(dirname "$log_path")"
  run_demo | tee -a "$log_path"
else
  run_demo
fi
