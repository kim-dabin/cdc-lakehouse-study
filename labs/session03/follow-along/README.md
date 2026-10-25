# 3회차 따라하기 — SQL로 변경하고 Bronze에서 확인하기

내 컴퓨터의 Docker 환경에서 주문·결제를 만든 뒤, **같은 환불을 20,000원 생성 → 30,000원 정정 → 삭제**합니다. 각 단계에서 원천의 현재 행, Kafka 이벤트, Bronze 이력을 비교합니다. 이번 실습은 Bronze까지이며 Silver·Gold를 갱신하지 않습니다.

명령은 **개인 컴퓨터의 저장소 루트에서 같은 Bash 터미널**에 한 블록씩 입력합니다. 강사 맥미니에 SSH로 접속하지 않습니다. 영상의 결과를 외우기보다 내 키와 값을 확인하세요. 설치·조회가 끝날 때까지 영상을 멈춰도 됩니다.

## 1. 저장소와 실행 환경 준비

Git, Python 3.9 이상, Docker Engine과 Compose v2가 필요합니다. 검증 기준은 Linux ARM64 Docker 환경입니다. 다른 CPU·운영체제의 전체 설치는 별도 확인이 필요합니다. Windows에서 Bash 명령을 실행한다면 WSL 터미널을 사용하고 Docker 연결을 확인합니다.

```bash
git --version
python3 --version
docker version
docker compose version
```

`docker version`에서 Server 연결 오류가 나면 Docker부터 켭니다. 처음 받는 경우에만 다음을 실행합니다.

```bash
git clone https://github.com/kim-dabin/cdc-lakehouse-study.git
cd cdc-lakehouse-study
pwd
ls
```

`compose.yaml`, `scripts`, `labs`가 보이면 저장소 루트입니다. 이미 받은 폴더가 있다면 그 폴더로 이동한 뒤 최신 실습 파일을 받습니다.

```bash
git pull --ff-only
```

수정 중인 파일 때문에 업데이트가 멈추면 오류를 남기고 확인합니다. 자신의 파일을 지우거나 강제로 초기화하지 않습니다. `labs/session03/follow-along`과 `compose.observers.yaml`이 있어야 이후 명령을 실행할 수 있습니다. 아래 환경 준비는 자신의 상태에 맞는 **한 가지**를 고릅니다.

| 현재 상태 | 할 일 |
|---|---|
| 처음 받은 폴더이고 실습 환경이 없음 | `.env`가 없을 때만 예제 복사 후 `bootstrap.sh --build` |
| 기존 환경의 서비스가 모두 중지됨 | 기존 `.env`를 유지하고 `bootstrap.sh --no-build` |
| 필요한 서비스가 이미 실행 중 | bootstrap을 반복하지 않고 preflight로 이동 |

새 폴더에 `.env`가 **없을 때만**:

```bash
cp .env.example .env
./scripts/bootstrap.sh --build
```

기존에 준비해 둔 환경이 **모두 중지돼 있을 때만**:

```bash
./scripts/bootstrap.sh --no-build
```

첫 빌드는 이미지 다운로드와 빌드에 시간이 걸립니다. bootstrap은 서비스·토픽·커넥터·Flink job·기준 데이터와 초기 Silver·Gold도 준비합니다. 오늘 새로 만드는 사건의 확인 범위는 Bronze입니다. `self-study.sh --environment first`는 환경 설치에 더해 자동 키로 c/u/d 전체를 실행하는 별도 경로이므로 이 수동 순서 중간에 끼우지 않습니다.

준비가 끝나면 다음을 실행합니다.

```bash
./scripts/lab.sh ps --all
./scripts/lab.sh exec -T runner python labs/session01/preflight.py
```

`ready=true`, `checkpoint_failures_during_check=0`과 최근 완료 checkpoint를 확인합니다. `./scripts/lab.sh`는 이 프로젝트의 Docker Compose 실행 도구이고, `exec -T runner`는 runner 컨테이너에서 명령을 실행한다는 뜻입니다. `-T`는 대화형 터미널을 별도로 만들지 않습니다. 일부 서비스만 실행되거나 preflight가 실패하면 오류와 마지막 명령을 남기고 [환경 안내](../../../docs/operations.md)를 확인합니다. 데이터 볼륨을 지우지 않습니다.

## 2. 새 키로 주문·결제 만들기

이번 실행의 주문·결제·환불을 묶는 키를 만듭니다. 출력한 값을 메모합니다. 새 터미널에서는 이 변수가 자동으로 이어지지 않습니다.

```bash
CASE_ID=s01-s03-my-$(date +%m%d%H%M%S)
printf 'CASE_ID=%s\n환불 ID=%s-r1\n' "$CASE_ID" "$CASE_ID"
```

먼저 [source-prepare.sql](source-prepare.sql)을 읽습니다.

```bash
cat labs/session03/follow-along/source-prepare.sql
```

`orders`의 `order_id`와 `payments`의 `payment_id`는 PK입니다. 결제의 `order_id`는 먼저 만든 주문을 가리키는 FK입니다. 그래서 주문 → 결제 순서로 넣습니다. 실제 스키마의 주문 시각 컬럼은 `ordered_at`이며, `amount`는 0 이상이어야 합니다. 두 INSERT를 `BEGIN`과 `COMMIT`으로 묶습니다.

이제 원천 PostgreSQL에서 SQL 파일을 실행합니다.

```bash
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/source-prepare.sql
./labs/session02/lab.sh "$CASE_ID" wait --event payment --evidence
```

`psql`은 PostgreSQL 실행 도구입니다. `-v case_id=...`로 받은 값을 SQL의 `:'case_id'`가 안전한 문자열로 사용합니다. `ON_ERROR_STOP=1`은 SQL 오류 뒤 계속 실행하지 않게 합니다. `< 파일`은 그 SQL 파일을 입력으로 넘깁니다.

기대 결과는 `INSERT 0 1` 두 번과 `COMMIT`, 이후 `bronze_visible=true`입니다. `wait`는 Bronze에 보일 때까지 조회하는 도구이며, 적재를 새로 시작하는 명령이 아닙니다.

원천을 직접 확인합니다.

```bash
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/source-check.sql
```

첫 결과는 테이블별 요약 **3행**이고 `current_rows`가 orders=1, payments=1, refunds=0입니다. 두 번째 환불 상세 결과는 0행입니다. 이 결과와 `bronze_visible=true`를 확인한 다음 진행합니다.

같은 키로 준비 SQL을 다시 실행하면 PK 충돌이 납니다. 응답을 못 받았더라도 먼저 읽기 전용 `source-check.sql`로 실제 상태를 확인합니다. 실패한 INSERT를 바로 반복하지 않습니다.

## 3. 환불 한 건의 생성·정정·삭제

아래 단계마다 SQL 파일을 먼저 읽고 실행합니다. `source-check.sql`은 읽기 전용이어서 반복 조회해도 됩니다. **변경 SQL은 각 단계에서 한 번씩만** 실행합니다.

### 3-1. 20,000원 환불 생성

[refund-insert.sql](refund-insert.sql)은 준비한 결제의 `payment_id`를 FK로 가진 환불 한 건을 넣습니다.

```bash
cat labs/session03/follow-along/refund-insert.sql
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/refund-insert.sql
./labs/session02/lab.sh "$CASE_ID" wait --event refund --evidence
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/source-check.sql
./labs/session02/lab.sh "$CASE_ID" kafka --summary --evidence
./labs/session02/lab.sh "$CASE_ID" bronze --summary --evidence
```

볼 값:

- 원천 환불 상세 1행, `amount=20000`, `status=success`.
- Kafka의 `op=c`, before 없음·after 금액20000.
- Bronze의 `record_count=1`, `history_ops=["c"]`.

Kafka의 topic·partition·offset은 실제 출력에서 읽습니다. 다른 사람의 번호와 같을 필요는 없습니다. 원천 1건·20,000원과 Bronze c 이력 1행을 확인한 뒤 넘어갑니다.

### 3-2. 같은 환불을 30,000원으로 정정

[refund-update.sql](refund-update.sql)은 새 환불을 추가하지 않고 같은 PK의 amount를 바꿉니다.

```bash
cat labs/session03/follow-along/refund-update.sql
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/refund-update.sql
./labs/session02/lab.sh "$CASE_ID" wait --event correction --evidence
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/source-check.sql
./labs/session02/lab.sh "$CASE_ID" kafka --summary --evidence
./labs/session02/lab.sh "$CASE_ID" bronze --summary --evidence
```

`UPDATE 1`, 원천 현재 환불1건·30000, Kafka u의 before20000·after30000, Bronze c/u 2행을 확인합니다. 원천 현재 행은 하나이고, Bronze에는 정정 이력이 추가됩니다.

같은 값으로 UPDATE를 반복해도 새 CDC 이벤트가 생길 수 있습니다. 기대한 2행 대신 이력이 늘었다면 추가 실행 여부를 확인하며, 맞추려고 기존 행이나 파일을 지우지 않습니다. `UPDATE 0`이면 같은 키의 환불이 있는지 먼저 확인합니다.

### 3-3. 환불 철회

[refund-delete.sql](refund-delete.sql)은 원천의 같은 환불 행을 삭제합니다. **이 예제에서 삭제는 환불 철회**를 뜻합니다. 결제·주문은 삭제하지 않습니다.

```bash
cat labs/session03/follow-along/refund-delete.sql
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/refund-delete.sql
./labs/session02/lab.sh "$CASE_ID" wait --event delete --evidence
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$CASE_ID" \
  < labs/session03/follow-along/source-check.sql
./labs/session02/lab.sh "$CASE_ID" kafka --summary --evidence
./labs/session02/lab.sh "$CASE_ID" bronze --summary --evidence
```

`DELETE 1` 뒤 같은 환불 키의 기대값입니다.

| 위치 | 결과 |
|---|---|
| 원천 refunds | 현재0건. 요약에서는 `current_rows=0`, 상세에서는0행 |
| Kafka | c/u/d와 value 전체 null인 tombstone, 누적4건 |
| Bronze | c/u/d 누적3행, tombstone은 제외 |

d 이벤트는 before에30000을 남기고 after가 null인 **본문이 있는 JSON**입니다. tombstone은 value 전체가 null인 별도 레코드입니다. Flink는 후자만 제외하고 d를 일반 이력 행으로 추가합니다. 이 d 행은 Iceberg DeleteFile이 아닙니다.

같은 키로 INSERT부터 다시 시작하지 않습니다. 다시 전체 흐름을 확인하려면 새 CASE_ID로 2절부터 별도 실행합니다. 기존 실행 기록은 남겨둡니다.

## 4. helper 안의 Bronze SQL도 직접 실행하기

앞의 `bronze`는 Spark SQL로 같은 환불을 조회한 결과를 정리해 보여줍니다. 이번에는 실제 SQL을 직접 넘깁니다. 호스트에 Spark를 새로 설치하지 않고 runner의 조회 프로그램을 사용합니다.

아래 블록은 CASE_ID가 허용된 형식일 때만 SQL 문자열에 넣습니다. 블록 전체를 복사합니다. 변수를 잃어버렸다면 먼저 메모한 자신의 CASE_ID를 복원하고, 새 키를 만들어 과거 결과를 찾지 않습니다.

```bash
if [[ "$CASE_ID" =~ ^s01-[a-z0-9-]{3,48}$ ]]; then
  ./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "
SELECT topic, partition_id, kafka_offset,
       get_json_object(payload, '\$.op') AS op,
       get_json_object(payload, '\$.before.refund_id') AS before_refund_id,
       get_json_object(payload, '\$.after.refund_id') AS after_refund_id,
       get_json_object(payload, '\$.before.amount') AS before_amount,
       get_json_object(payload, '\$.after.amount') AS after_amount,
       _file AS parquet_file
FROM lake.bronze.cdc_events
WHERE topic = 'cdc.public.refunds'
  AND (get_json_object(payload, '\$.before.refund_id') = '${CASE_ID}-r1'
       OR get_json_object(payload, '\$.after.refund_id') = '${CASE_ID}-r1')
ORDER BY partition_id, kafka_offset"
else
  printf '메모한 CASE_ID를 먼저 설정하세요. 예: s01-으로 시작하는 내 실행 키\n' >&2
fi
```

터미널의 큰따옴표 안에서는 `${CASE_ID}`가 자신의 값으로 바뀝니다. `\$`는 JSON 경로의 `$`가 SQL로 그대로 전달되도록 합니다. 두 PK 조건을 OR로 묶는 이유는 삭제 이벤트의 after에는 환불 ID가 없기 때문입니다.

원천 PostgreSQL에서 이 SQL을 실행하지 않습니다. 결과는 앞의 Bronze helper와 같은 c/u/d 3행이고, 각 행의 `parquet_file`은 실제 읽은 파일 경로입니다. 세 이벤트가 꼭 파일 세 개라는 뜻은 아닙니다.

선택적으로 SQL 편집기를 쓴다면 [bronze-check.sql](bronze-check.sql)을 열고 `__REFUND_ID__` **두 곳 모두** 자신의 환불 ID로 바꿉니다. 다음 출력값을 복사하며, SQL의 바깥 작은따옴표는 유지합니다.

```bash
printf 'SQL의 __REFUND_ID__ 두 곳에 넣을 값: %s-r1\n' "$CASE_ID"
```

이 템플릿을 치환 없이 실행하거나 psql에 입력하지 않습니다. DBeaver는 선택 도구이며 위 터미널 경로만으로 같은 SQL 결과를 확인할 수 있습니다.

## 5. 실제 파일·메타데이터로 이어서 확인하기

다음은 읽기 전용입니다.

```bash
./labs/session02/lab.sh "$CASE_ID" files --summary --evidence
./labs/session02/lab.sh "$CASE_ID" parquet --summary --evidence
./labs/session03/inspect.sh "$CASE_ID"
```

`files`는 해당 환불 행이 읽힌 파일의 경로·행 수·바이트 크기를, `parquet`는 실제 저장소의 bytes를 읽어 디코딩한 내용과 SQL 결과의 일치 여부를 보여줍니다. `passed=true`와 `matching_record_count=3`을 확인합니다. 파일 전체 `record_count`와 이 환불의 이력 수는 다른 범위입니다.

이어서 [화면에서 확인하고 새 환불의 지연 재기](screens-and-latency.md)의 7~14절을 따라갑니다. Kafka UI 기동·검색, Flink checkpoint, Catalog → MinIO JSON·Parquet, 별도 새 키의 지연 측정 명령을 순서대로 제공합니다. 개념을 더 읽으려면 [3회차 가이드의 관찰 B·C](../student-guide.md#6-raw-이력에서-minio와-테이블-조회까지-연결하기)를 함께 봅니다. 파일을 수동 삭제하거나 가장 최근 파일명만 보고 current라고 판단하지 않습니다.

**지연 측정에는 별도의 새 CASE_ID가 필요합니다.** 여기서 끝낸 키에는 이미 환불 이력이 있으므로 그대로 `measure-latency.sh --insert-refund`에 넣지 않습니다. 새 주문·결제만 준비하고 환불은 측정 도구 안에서 한 번 입력합니다.

## 6. 결과를 남기고 종료하기

CASE_ID, 확인한 원천/Kafka/Bronze 값, 마지막으로 성공한 단계와 질문 하나를 남깁니다. `--evidence` 결과는 `artifacts/runs/session02/<CASE_ID>/` 아래에 생깁니다. 같은 action을 다시 조회하면 같은 이름의 결과 파일이 갱신되므로 생성·정정·삭제 각각의 화면을 남기려면 단계별로 캡처합니다.

별도 자동 검증이 필요하다면 [개인 실행 안내](../../../docs/self-study.md)의 `self-study.sh --environment reuse`를 사용할 수 있습니다. 이는 새 자동 키로 전체 공통 예제를 실행하는 별도 실습이며, 지금 키의 중간 단계만 재개하는 명령이 아닙니다.

끝나면 다음 명령으로 savepoint를 남기며 중지합니다.

```bash
./scripts/stop.sh
```

SQL 오류나 timeout이면 출력과 현재 상태를 먼저 확인합니다. 네트워크 오류가 나도 원천에서 이미 commit됐을 수 있으므로 DML을 바로 반복하지 않습니다. `down -v`, 볼륨 삭제, Kafka offset 초기화로 결과를 맞추지 않습니다.
