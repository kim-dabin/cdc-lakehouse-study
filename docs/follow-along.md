# 1~7회차 통합 따라 하기 — 빈 Docker 환경에서 7회차 실험까지

개인 Docker 환경에서 1회차부터 7회차까지 다룬 파이프라인과 격리 실험을 처음부터 순서대로 실행하고 확인하는 안내서입니다. 학생은 자기 컴퓨터에서 각 단계의 명령을 실행하고, 강사는 화면 공유를 보며 **화면 체크포인트**의 문구와 숫자가 나오는지 바로 대조합니다.

수업 중 별도 제출 절차는 없으며 과제 형식과 기한은 별도 공지를 따릅니다.

---

## 0. 시작 전

### 필요 사양과 사전 조건

- **필수 도구**: Git, Bash, Python 3.9 이상, `curl`, Docker Engine, Docker Compose v2 ([`self-study.md`](self-study.md), [`../labs/session03/follow-along/README.md`](../labs/session03/follow-along/README.md))
- **컨테이너 환경**: Linux ARM64 컨테이너 실행 환경 기준입니다. 다른 CPU 아키텍처의 이미지 호환성과 실행 성능은 별도 확인이 필요합니다. Windows에서는 WSL 터미널을 사용하고 Docker 연결 상태를 먼저 확인합니다.
- **참고 자원 수치**: 스터디 공통 환경 실측에 사용한 기준 구성은 **Linux ARM64 Docker VM, CPU 6개·RAM 10 GiB**입니다([`self-study.md`](self-study.md)). 최초 이미지 빌드와 다운로드에는 네트워크 연결과 디스크 여유 공간이 필요합니다.
- **선택 스택 추가 자원**: 6회차 선택 항목인 OpenMetadata를 켤 때는 서버·검색·DB에 메모리 약 **3 GB**(스터디 공통 환경 실측 2026-11-01: 서버 1.3 GiB, Elasticsearch 1.4 GiB, DB 0.2 GiB)와 수집 이미지(`openmetadata/ingestion:2.0.5`) 디스크 **7.7 GB**가 추가로 필요합니다([`governance.md`](governance.md)).

### 저장소 받기와 최신 코드 반영

모든 명령은 **개인 컴퓨터의 저장소 루트 디렉터리**에서 실행합니다.

```bash
# 처음 클론할 때
git clone https://github.com/kim-dabin/cdc-lakehouse-study.git
cd cdc-lakehouse-study

# 이미 클론한 폴더가 있을 때
cd cdc-lakehouse-study
git pull --ff-only
pwd
ls
```

`compose.yaml`, `scripts`, `labs`, `docs`가 보이면 저장소 루트입니다.

> **기존 환경 주의 (2026-11-01 이전 생성 환경)**: 2026-11-01부터 공통 Bronze는 토픽별 테이블(`lake.bronze.orders`·`payments`·`refunds`)을 사용합니다. 이 날짜 이전에 만든 환경이라면 최신 코드를 받은 뒤 `./scripts/migrate-bronze-per-topic.sh`를 먼저 1회 실행합니다([`operations.md`](operations.md#bronze-토픽별-테이블-전환-기존-환경-1회)). 새로 설치하는 환경은 해당하지 않습니다.

### 실행 순서 한눈에 보기

| 단계 | 회차 | 핵심 명령 | 걸리는 시간 (문서·실측 기준) | 핵심 체크포인트 |
|---|---|---|---|---|
| **1. 환경 준비·기동** | 1~3회차 | `./scripts/self-study.sh --plan`<br>`./scripts/self-study.sh --environment first` | 첫 빌드는 이미지 다운로드·빌드 시간 소요 (환경별 상이) | 터미널 `결과: passed`, Flink UI(`:28081`) `cdc-bronze` `RUNNING`, `ready: true` |
| **2. 수집·Bronze 대조** | 2~3회차 | `./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --no-deps kafka-ui`<br>`./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --build --no-deps explorer` | 즉시 (첫 이미지 빌드·다운로드 제외) | `report.md` 원천 `1→1→0` / Kafka `1→2→4` / Bronze `1→2→3`, Kafka UI(`:28082`), MinIO(`:29001`), Explorer(`:28090`) |
| **3. Silver 현재 상태 실험** | 4회차 | `./labs/session04/lab.sh all --run-id "$RUN_ID"` | 문서 내 별도 시간 수치 없음 | `"step": "verify", "passed": true`, `stored_rows` `["refund-r1", 30000, true, 300, 12]`, `current_business_row_count: 0` |
| **4. dbt Silver core·Gold** | 5회차 | `./scripts/self-study.sh --environment reuse --with-marts`<br>`./scripts/verify.sh` | 문서 내 별도 시간 수치 없음 | `dbt build` `Done. PASS=20 WARN=0 ERROR=0 SKIP=0`, 순결제액 `100000→80000→70000→100000`, `PASS: artifacts/runs/verification-...json` |
| **5. (선택) 거버넌스·OM** | 6회차 앞부분 | `./governance/contract-failure-demo.sh`<br>`./scripts/governance.sh` | 처음 실행 시 이미지 다운로드 수 분 소요 | 계약 위반 `amount \| BIGINT_TYPE \| STRING_TYPE`, OM(`:28585`) 계약 5개 `Success`·컬럼 리니지 |
| **6. 증분·소급 정정 실험** | 6회차 | `./labs/session06/demo.sh --fresh`<br>`./labs/session06/lab.sh all --run-id "$RUN_ID"` | 문서 내 별도 시간 수치 없음 | Explorer 6회차 예제 12개(`s5` `11-01` 삭제·`10-28` 이동, `s6` 정상 vs `naive` 차이 1행 `2026-10-28`) |
| **7. (선택) Airflow 화면** | 6회차 설계 | `./scripts/lab.sh --profile orchestration up -d airflow` | 문서 내 별도 시간 수치 없음 | Airflow UI(`:28080`) `lakehouse_refresh` DAG **Paused 유지**(Unpause 하지 않음), `schedule='*/5 * * * *'` |
| **8. Iceberg 유지보수 실험** | 7회차 | `./labs/session07/demo.sh --fresh`<br>`./labs/session07/lab.sh all --run-id "$RUN_ID"` | `demo.sh` 약 90초, `lab.sh all` 약 27초 (스터디 공통 환경 실측 2026-11-07) | Explorer 7회차 예제 8개(`grow` `120`·`101` vs `11`, `compact` `1` vs `121`, `expire` `1`, `clean` `1`/`23`, `trap` `1`) |
| **9. 정리와 끄기** | 전체·7회차 | `./labs/session06/lab.sh clean ...`<br>`./labs/session07/lab.sh clean ...`<br>`./scripts/stop.sh` | 즉시 | 실험 `run-id` 테이블 `DROP TABLE PURGE`, Flink savepoint 저장 후 컨테이너 중지 |

---

## 단계 1. 환경 준비와 전체 파이프라인 기동 (1~3회차)

### ① 무엇을 확인하나
공통 실행 도우미로 Docker 환경을 기동해 PostgreSQL·Debezium·Kafka·Flink(`cdc-bronze`)·Spark·MinIO 상시 서비스를 준비하고, 새 환불 키의 생성·정정·삭제 수집을 끝까지 실행합니다(1~3회차).

### ② 명령
먼저 실행 계획을 읽은 뒤, **현재 환경 상태에 맞는 명령 하나만** 실행합니다. 세 모드를 차례로 실행하지 않습니다.

```bash
# 실행 계획만 출력 (Docker 접속·파일 생성 없음)
./scripts/self-study.sh --plan
```

```bash
# [선택 A] 처음 설치하는 빈 환경일 때 (.env 자동 준비 + 이미지 빌드 + 기동 + 공통 예제 실행)
./scripts/self-study.sh --environment first

# [선택 B] 이전에 준비해 둔 컨테이너가 모두 중지된 상태일 때 (로컬 이미지로 재기동)
./scripts/self-study.sh --environment start

# [선택 C] 필수 컨테이너 9개가 이미 실행 중일 때 (재기동 없이 새 키로 공통 예제만 실행)
./scripts/self-study.sh --environment reuse
```

기동 후 서비스 상태와 준비 상태를 직접 점검할 때는 아래 명령을 사용합니다.

```bash
./scripts/lab.sh ps --all
./scripts/lab.sh exec -T runner python labs/session01/preflight.py
```

### ③ 화면 체크포인트
- **터미널 (`self-study.sh` 출력)**:
  - 실행 중 각 환불 단계마다 `확인: 원천 1행 / Kafka 1건 / Bronze 1건 / 실제 Parquet 일치` → `원천 1행 / Kafka 2건 / Bronze 2건` → `원천 0행 / Kafka 4건 / Bronze 3건`이 차례로 출력됩니다.
  - 마지막 두 줄에 `결과: passed`와 `보고서: .../artifacts/runs/self-study/<CASE_ID>/report.md` 경로가 찍힙니다.
- **터미널 (`preflight.py` 출력)**:
  - JSON 출력에서 `"ready": true`, `"checkpoint_failures_during_check": 0`, 완료된 체크포인트 수(`"completed"` ≥ 1)가 보입니다.
- **브라우저 Flink UI (`http://localhost:28081`)**:
  - **Running Jobs**에 `cdc-bronze` 작업 1개가 `RUNNING` 상태이고, **Checkpoints** 탭에서 `Completed` 숫자가 올라가며 `Failed`가 증가하지 않는지 확인합니다.

### ④ 막히면
- **`Some project services are active. Use --environment reuse or inspect the partial startup`**: 일부 컨테이너만 켜진 상태에서 `first`나 `start`를 실행한 경우입니다. `./scripts/lab.sh ps --all`로 상태를 확인하고 `./scripts/bootstrap.sh --no-build`로 마저 기동한 뒤 `--environment reuse`를 사용합니다.
- **MinIO 이미지 다운로드에서 `unauthorized` 오류**: `git pull --ff-only`로 SILO(`pgsty/silo`, `pgsty/mc`) 이미지가 반영된 최신 코드를 받은 뒤 `./scripts/bootstrap.sh --build`를 실행합니다. 볼륨 삭제(`down -v`)나 `.env` 초기화는 하지 않습니다.
- **`Airflow is active`**: 주기 실행과 격리 검증이 겹치지 않도록 `./scripts/lab.sh --profile orchestration stop airflow`로 중지한 뒤 다시 실행합니다.
- **자세한 안내**: [`self-study.md`](self-study.md), [`operations.md`](operations.md), [`../labs/session01/student-guide.md`](../labs/session01/student-guide.md)

---

## 단계 2. 수집·Bronze 토픽별 테이블·실제 파일 대조 (2~3회차)

### ① 무엇을 확인하나
단계 1에서 생성된 보고서와 SQL·웹 UI(Kafka UI·MinIO·Lakehouse Explorer)로 원천 변경이 Kafka 토픽 3개와 Bronze 토픽별 테이블 3개(`orders`·`payments`·`refunds`), 그리고 MinIO의 실제 Parquet·`metadata.json` 파일에 어떻게 남았는지 대조합니다(2~3회차).

### ② 명령
방금 실행된 최신 보고서를 확인하고, 관찰용 웹 UI(Kafka UI, Lakehouse Explorer)를 띄웁니다.

```bash
# 가장 최근 self-study 보고서 확인
ls -dt artifacts/runs/self-study/s01-* | head -n 1
cat "$(ls -dt artifacts/runs/self-study/s01-* | head -n 1)/report.md"
```

```bash
# 원천 PostgreSQL의 논리 복제·publication·slot 설정과 Debezium 커넥터 설정 확인
./labs/session02/lab.sh source-config
./labs/session02/lab.sh connector-sanitized
```

```bash
# 선택 관찰 화면 기동: Kafka UI(:28082)와 Lakehouse Explorer(:28090)
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --no-deps kafka-ui
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --build --no-deps explorer
```

```bash
# 카탈로그 DB에서 bronze.refunds의 현재 metadata.json 위치 확인
./scripts/lab.sh exec -T catalog-db psql -X -v ON_ERROR_STOP=1 \
  -U catalog -d catalog -c "
SELECT metadata_location FROM iceberg_tables
WHERE table_namespace='bronze' AND table_name='refunds';"
```

### ③ 화면 체크포인트
- **터미널 (`report.md` 표)**:
  - `CASE_ID: s01-self-...`, `결과: **passed**`가 표시됩니다.
  - 단계별 표에서 아래 숫자가 정확히 일치해야 합니다.
    - `refund`(20,000원 생성): 원천 환불 행 `1` | 원천 금액 `20000` | Kafka `1` | Bronze 이력 `1`
    - `correction`(30,000원 정정): 원천 환불 행 `1` | 원천 금액 `30000` | Kafka `2` | Bronze 이력 `2`
    - `delete`(환불 철회): 원천 환불 행 `0` | 원천 금액 `None` | Kafka `4` | Bronze 이력 `3`
- **터미널 (`source-config` · `connector-sanitized`)**:
  - `wal_level=logical`, publication `cdc_lab`(`orders`, `payments`, `refunds`), 세 테이블 replica identity `f`(FULL), slot `cdc_lab`(`pgoutput`, `active=true`), `topic.prefix=cdc`를 확인합니다.
- **브라우저 Kafka UI (`http://localhost:28082`)**:
  - **Topics → `cdc.public.refunds` → Messages**에서 위 `CASE_ID`의 환불 ID(`<CASE_ID>-r1`)로 검색하면 `op=c`(after `20000`), `op=u`(before `20000`, after `30000`), `op=d`(before `30000`, after `null`), 그리고 value 전체가 `null`인 **tombstone**까지 총 **4건**이 보입니다.
- **브라우저 Lakehouse Explorer (`http://localhost:28090`)**:
  - 상단 상태 표시줄에 `Spark Thrift 정상`, `MinIO 정상`이 초록색으로 표시됩니다.
  - **SQL** 탭 왼쪽 예제에서 `bronze 테이블`을 실행하면 `orders`, `payments`, `refunds` 3개 테이블이 나오고, `Bronze 토픽별 건수`를 실행하면 세 테이블의 행 수가 조회됩니다.
- **브라우저 MinIO (`http://localhost:29001`, 로그인은 로컬 `.env`의 `S3_ACCESS_KEY` / `S3_SECRET_KEY`)**:
  - **Object Browser → `warehouse` → `bronze/refunds/metadata/`**에서 카탈로그 DB가 가리키는 `metadata.json` 객체를 확인하고, `bronze/refunds/data/` 아래에 실제 `.parquet` 파일이 존재하는지 확인합니다.

### ④ 막히면
- **Bronze 조회에서 DELETE(`op=d`) 행만 안 보일 때**: DELETE 이벤트는 `after`가 `null`이고 `before.refund_id`에 키가 들어 있습니다. WHERE 조건에 `before.refund_id`와 `after.refund_id`를 모두 포함했는지 확인합니다.
- **수동 SQL로 한 단계씩 직접 넣고 지연(`measure-latency.sh`)까지 재고 싶을 때**: [`../labs/session03/follow-along/README.md`](../labs/session03/follow-along/README.md)와 [`../labs/session03/follow-along/screens-and-latency.md`](../labs/session03/follow-along/screens-and-latency.md)를 따라 진행합니다.
- **자세한 안내**: [`../labs/session02/student-guide.md`](../labs/session02/student-guide.md), [`../labs/session03/student-guide.md`](../labs/session03/student-guide.md)

---

## 단계 3. Silver 현재 상태와 `lesson04` 실험 (4회차)

### ① 무엇을 확인하나
격리 실험판(`lake.lesson04`)의 고정 입력으로 `ROW_NUMBER` 최신 후보 선택, LSN·offset 버전 가드 `MERGE`, 삭제 표시(`is_deleted=true`) 보존, 중복 전달과 삭제 뒤 도착한 과거 UPDATE 거절을 검증합니다(4회차).

### ② 명령
새 `RUN_ID`를 만들어 전체 단계(`prepare` → `latest` → `merge c/u/d` → `replay` → `verify`)를 실행합니다.

```bash
RUN_ID="s04-$(date +%Y%m%d-%H%M%S)"
echo "RUN_ID=$RUN_ID"
./labs/session04/lab.sh all --run-id "$RUN_ID"
```

같은 입력을 다시 MERGE했을 때 업무 결과 유지와 Copy-on-Write 파일 재작성을 확인하려면 이어서 실행합니다.

```bash
./labs/session04/lab.sh merge --stage all --run-id "$RUN_ID"
./labs/session04/lab.sh verify --run-id "$RUN_ID"
```

### ③ 화면 체크포인트
- **터미널 (단계별 JSON 줄 출력)**:
  - `merge` 단계 변화:
    - `"stage": "c"` → `"stored_rows": [["refund-r1", 20000, false, 100, 10]]`, `"current_rows": [["refund-r1", 20000]]`
    - `"stage": "u"` → `"stored_rows": [["refund-r1", 30000, false, 200, 11]]`, `"current_rows": [["refund-r1", 30000]]`
    - `"stage": "d"` → `"stored_rows": [["refund-r1", 30000, true, 300, 12]]`, `"current_rows": []`
  - `old_update_replay` 줄: `"incoming_lsn": 200, "incoming_kafka_offset": 13`, `"target_lsn": 300, "target_kafka_offset": 12`, **`"guard_rejected": true`**
  - `verify` 줄: **`"step": "verify", "passed": true`**, `"stored_rows": [["refund-r1", 30000, true, 300, 12]]`, **`"current_business_row_count": 0`**, `"bronze_delivery_duplicates": [["cdc.public.refunds", 0, 12, 2]]`
  - 마지막 줄에 `REPORT: .runtime/session04/<RUN_ID>/report.json`이 출력됩니다.

### ④ 막히면
- **`ValueError: 이 run-id의 Silver가 이미 존재합니다. all은 새 run-id로 시작하세요.`**: 이미 완료한 `RUN_ID`로 `all`을 다시 실행한 경우입니다. 새 `RUN_ID`를 만들어 실행하거나, 동일 입력 재실행은 `merge --stage all`을 사용합니다.
- **단계별로 끊어서 예측하며 실행하고 싶을 때**: [`../labs/session04/follow-along/README.md`](../labs/session04/follow-along/README.md)와 [`../labs/session04/student-guide.md`](../labs/session04/student-guide.md)를 참고합니다.

---

## 단계 4. dbt Silver core와 Gold 마트 검증 (5회차)

### ① 무엇을 확인하나
Silver 현재 상태 위에 dbt로 Silver core(`dim_order`, `fct_payment`, `fct_refund`)와 Gold 마트(`mart_order_settlement`, `mart_daily_settlement`)를 빌드·테스트하고, 환불 생성·정정·철회에 따른 순결제액(`net_paid`) 변화를 검증합니다(5회차).

### ② 명령
dbt 모델·테스트 목록을 확인한 뒤, `--with-marts` 옵션으로 단계별 마트 갱신을 검증하거나 `verify.sh`로 전체 파이프라인(중복·과거 UPDATE 재전달 포함)을 검증합니다.

```bash
# dbt 모델 5개·테스트 15개·원천 3개 목록과 데이터 테스트 실행 확인
./scripts/lab.sh exec -T runner dbt ls --project-dir dbt --profiles-dir dbt --output name
./scripts/lab.sh exec -T runner dbt test --project-dir dbt --profiles-dir dbt
```

```bash
# 새 공통 예제 키로 환불 생성·정정·삭제마다 Silver·dbt·Gold 순결제액까지 검증
./scripts/self-study.sh --environment reuse --with-marts
```

```bash
# 원천 변경 + 중복 전달 + 삭제 뒤 과거 UPDATE 재전달까지 Gold에 반영해 검증
./scripts/verify.sh
```

### ③ 화면 체크포인트
- **터미널 (`dbt ls` · `dbt test`)**:
  - `Found 5 models, 15 data tests, 3 sources`가 표시되고 데이터 테스트 15개가 모두 통과합니다(전체 `dbt build`는 모델 5개 포함 `PASS=20`, 스터디 공통 환경 실측 2026-11-01).
- **터미널 (`self-study.sh --environment reuse --with-marts` 보고서)**:
  - `cat "$(ls -dt artifacts/runs/self-study/s01-* | head -n 1)/report.md"`에서 **마트 갱신 후 순결제액** 열이 결제 직후 `100000` → `refund` `80000` → `correction` `70000` → `delete` `100000`으로 찍히고 `결과: **passed**`가 나옵니다.
- **터미널 (`./scripts/verify.sh` 출력)**:
  - 6개 검증 단계 JSON 줄이 차례로 출력됩니다:
    1. `"case": "insert", "expected_net_paid": 100000, "actual_net_paid": 100000`
    2. `"case": "refund_insert", "expected_net_paid": 80000, "actual_net_paid": 80000`
    3. `"case": "duplicate_replay", "expected_net_paid": 80000, "actual_net_paid": 80000`
    4. `"case": "refund_update", "expected_net_paid": 70000, "actual_net_paid": 70000`
    5. `"case": "refund_delete", "expected_net_paid": 100000, "actual_net_paid": 100000`
    6. `"case": "old_update_after_delete", "expected_net_paid": 100000, "actual_net_paid": 100000`
  - 마지막 줄에 `PASS: artifacts/runs/verification-<타임스탬프>.json`이 출력됩니다.
- **브라우저 Lakehouse Explorer (`http://localhost:28090`)**:
  - **SQL** 탭에서 `Silver core · 주문 dim`, `Silver core · 결제 fact`, `Silver core · 환불 fact`, `Gold 주문 마트`, `Gold 일별 마트` 예제를 눌러 결과를 확인합니다.

### ④ 막히면
- **`Bronze is still the single table lake.bronze.cdc_events`**: 2026-11-01 이전 환경에서 토픽별 Bronze 전환을 아직 실행하지 않은 경우입니다. `./scripts/migrate-bronze-per-topic.sh`를 실행합니다([`operations.md`](operations.md#bronze-토픽별-테이블-전환-기존-환경-1회)).
- **자세한 안내**: [`../labs/session05/student-guide.md`](../labs/session05/student-guide.md), [`self-study.md`](self-study.md)

---

## 단계 5. (선택) 거버넌스: 모델 계약과 OpenMetadata 컬럼 리니지 (6회차 앞부분)

### ① 무엇을 확인하나
dbt 모델 계약(`contract: {enforced: true}`)이 열 타입 불일치를 테이블 교체 전에 막는지 확인하고, 선택 스택인 OpenMetadata에서 데이터 계약과 컬럼 단위 리니지를 확인합니다(6회차 앞부분).

> **메모리·디스크 경고**: OpenMetadata 스택은 서버·검색(Elasticsearch)·전용 DB를 합쳐 메모리를 약 **3 GB** 더 사용하며(스터디 공통 환경 실측 2026-11-01: 서버 1.3 GiB, Elasticsearch 1.4 GiB, DB 0.2 GiB), 수집 이미지(`openmetadata/ingestion:2.0.5`) 크기가 **7.7 GB**입니다([`governance.md`](governance.md)). 여유 자원이 부족하면 `contract-failure-demo.sh`만 실행하고 OpenMetadata 기동은 건너뛰어도 됩니다.

### ② 명령
모델 계약 실패 시연은 추가 컨테이너 없이 바로 실행할 수 있습니다.

```bash
# 원본 dbt/ 폴더를 건드리지 않고 복사본(.runtime/contract-demo)에서 계약 실패 시연
./governance/contract-failure-demo.sh
```

메모리와 디스크 여유가 있을 때만 아래 OpenMetadata 선택 스택을 실행합니다.

```bash
# 1) OpenMetadata 서버·검색·DB 기동 (첫 실행 시 이미지 다운로드에 몇 분 소요)
./scripts/lab.sh -f compose.yaml -f compose.governance.yaml --profile governance up -d om-server

# 2) dbt build → Spark 메타데이터 수집 → dbt 수집 → 데이터 계약 5개 → 컬럼 리니지 등록
#    (단계 4에서 이미 dbt build를 마쳤다면 ./scripts/governance.sh --no-build 사용 가능)
./scripts/governance.sh
```

확인이 끝나면 메모리를 돌려주기 위해 OpenMetadata 서비스를 중지합니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.governance.yaml --profile governance stop om-server om-search om-db
```

### ③ 화면 체크포인트
- **터미널 (`contract-failure-demo.sh` 출력)**:
  - `This model has an enforced contract that failed.` 문구와 함께 `amount | BIGINT_TYPE | STRING_TYPE | data type mismatch` 표가 출력되며, 기존 `lake.silver.fct_payment` 테이블은 바뀌지 않습니다.
- **터미널 (`governance.sh` 출력)**:
  - 마지막 줄에 `OpenMetadata: http://127.0.0.1:28585 (service cdc_lakehouse)`가 출력됩니다(스터디 공통 환경 실측 2026-11-01: 메타데이터 수집 18건, dbt 수집 24건, 계약 5개 검증 `Success`).
- **브라우저 OpenMetadata (`http://localhost:28585`, 로그인 `admin@open-metadata.org` / `admin`)**:
  - `http://localhost:28585/table/cdc_lakehouse.default.gold.mart_order_settlement/lineage`로 이동해 **Lineage** 탭에서 각 노드의 **Columns**를 펼치면 `silver.fct_payment.amount` → `paid`, `net_paid` 및 `silver.fct_refund.amount` → `refunded`, `net_paid` 연결선이 보입니다.
  - **Contract** 탭에서 계약 상태 `Success`, **Data Quality** 탭에서 dbt 테스트 통과 결과를 확인합니다.

### ④ 막히면
- **OpenMetadata 컨테이너 기동 직후 수집 스크립트가 실패할 때**: `om-server`의 healthcheck가 `healthy`가 될 때까지(`./scripts/lab.sh -f compose.yaml -f compose.governance.yaml ps`) 잠시 기다린 뒤 `./scripts/governance.sh --no-build`를 다시 실행합니다.
- **자세한 안내**: [`governance.md`](governance.md), [`../labs/session06/student-guide.md`](../labs/session06/student-guide.md#1-거버넌스-모델-계약과-컬럼-리니지)

---

## 단계 6. `lesson06` 증분·소급 정정·재시도 실험 (6회차)

### ① 무엇을 확인하나
격리 실험판(`lake.lesson06`)에서 `create_changelog_view`(COW carry-over 제거), 하위→상위 키 전파(`r1 → p1 → o1`), 과거 주문일 소급 정정(`s5`), 중간 실패 후 재시도 시 `plan` 선저장의 역할(`s6` 정상 모드 대 `--naive-dates`)을 검증합니다(6회차).

### ② 명령
먼저 고정 데모 6세트(`s06-demo-s2`~`s6`, `s06-demo-naive`)를 준비해 Explorer 예제를 열어 보고, 이어서 개인 `RUN_ID`로 전체 시나리오를 실행합니다.

```bash
# Explorer 예제용 고정 데모 6세트 준비 (이미 있으면 --fresh로 재생성)
./labs/session06/demo.sh --fresh

# Explorer가 꺼져 있다면 기동
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --build --no-deps explorer
```

```bash
# 개인 RUN_ID로 s1~s6 정상 모드 연속 실행
RUN_ID="s06-all-$(date +%Y%m%d-%H%M%S)"
./labs/session06/lab.sh all --run-id "$RUN_ID"
```

### ③ 화면 체크포인트
- **터미널 (`lab.sh all` 출력)**:
  - `s1`~`s6` 각 단계의 `"step": "scenario_result"` 줄에서 모두 **`"match": true, "fixed_expected_match": true, "expected_match": true, "passed": true`**가 출력됩니다.
  - `s5`의 `"step": "carryover", "table": "orders"` 줄에서 `"raw_rows": 4, "raw_keys": ["o1", "o2"], "changelog_rows": 2, "changelog_keys": ["o1"], "carryover_keys": ["o2"]`를 확인합니다.
  - `s6`에서 1차 시도 `"step": "injected_failure"` 뒤 `"step": "retry", "progress_unchanged": true`, 이어서 `"step": "plan", "reused": true, "dates": ["2026-10-25", "2026-10-28"]`가 출력됩니다.
- **브라우저 Lakehouse Explorer (`http://localhost:28090` → SQL 탭 `6회차 · ...` 예제 12개 중 핵심 체크포인트)**:
  - `6회차 · s5 원본 changes · o2 carry-over`: `s5` 커밋에서 실제 변경된 `o1` 2행뿐 아니라 같은 파일에 있던 `o2`(LSN `100` 그대로) 2행까지 총 4행이 조회됩니다.
  - `6회차 · s5 일별 Gold · 날짜 소급 정정`: `2026-11-01` 행이 삭제되고 **2행**만 남습니다:
    - `2026-10-28` | `order_count=1` | `paid=100000` | `refunded=0` | `net_paid=100000`
    - `2026-11-02` | `order_count=2` | `paid=50000` | `refunded=0` | `net_paid=50000`
  - `6회차 · s6 정상 일별 Gold`: `2026-10-28` 행이 삭제되고 **2행**만 남습니다:
    - `2026-10-25` | `order_count=1` | `paid=100000` | `refunded=0` | `net_paid=100000`
    - `2026-11-02` | `order_count=2` | `paid=50000` | `refunded=0` | `net_paid=50000`
  - `6회차 · s6 naive 일별 Gold`: 재시도 때 옛 날짜(`2026-10-28`)를 놓쳐 `2026-10-25`, `2026-10-28`, `2026-11-02` **3행**이 조회됩니다.
  - `6회차 · s6 naive와 전체 재계산 차이`: 양방향 `EXCEPT` 결과로 **딱 1행**이 나옵니다:
    - `difference='actual_minus_full'` | `order_date='2026-10-28'` | `order_count=1` | `paid=100000` | `refunded=0` | `net_paid=100000`

### ④ 막히면
- **`이미 있는 데모 run-id: ... 새로 준비하려면 demo.sh --fresh를 쓰세요.`**: `./labs/session06/demo.sh --fresh`를 실행해 데모 테이블 6세트만 다시 만듭니다.
- **`ValueError: run-id: s06-로 시작하고 소문자/숫자/_/-만 사용하세요(최대 40자).`**: `--run-id`가 `s06-`으로 시작하는지 확인합니다.
- **자세한 안내**: [`../labs/session06/student-guide.md`](../labs/session06/student-guide.md#6-집에서-따라-하기), [`../tools/explorer/README.md`](../tools/explorer/README.md)

---

## 단계 7. (선택) Airflow 화면과 스케줄 설계 확인 (6회차)

### ① 무엇을 확인하나
`orchestration` 프로필로 Airflow를 띄워 `lakehouse_refresh` DAG의 스케줄·옵션(`schedule='*/5 * * * *'`, `max_active_runs=1`, `catchup=False`, `is_paused_upon_creation=True`)을 화면과 코드로 확인합니다(6회차).

> **주의 — DAG를 Unpause 하지 마세요**: `lakehouse_refresh`를 Unpause하면 5분마다 공통 Silver와 Gold를 다시 빌드해 다른 실습 쿼리와 Spark Thrift 자원을 공유하게 됩니다. 이번 단계에서는 **일시정지(Paused) 상태 그대로** 화면과 설정만 확인합니다.

### ② 명령
```bash
# Airflow 기동 및 DAG import 오류 유무 확인
./scripts/lab.sh --profile orchestration up -d airflow
./scripts/lab.sh exec -T airflow airflow dags list-import-errors

# 초기 admin 비밀번호 확인 (화면에만 출력하고 외부에 공유하지 않음)
./scripts/lab.sh exec -T airflow cat /opt/airflow/standalone_admin_password.txt
```

### ③ 화면 체크포인트
- **터미널 (`list-import-errors` 출력)**:
  - `No data found`가 출력되어야 합니다.
- **브라우저 Airflow UI (`http://localhost:28080`, 계정 `admin` / 위에서 확인한 초기 비밀번호)**:
  - DAG 목록에 `lakehouse_refresh`가 보이며, 왼쪽 토글이 **Paused(꺼짐)** 상태입니다(Unpause 하지 않음).
  - 스케줄이 `*/5 * * * *`로 표시되고, DAG를 눌러 **Graph** 또는 **Code** 탭을 열면 `silver_then_dbt_build` 태스크 1개와 `max_active_runs=1`, `catchup=False`, `retries=0` 설정([`../airflow/dags/lakehouse_refresh.py`](../airflow/dags/lakehouse_refresh.py))을 확인할 수 있습니다.

### ④ 막히면
- **웹 UI가 바로 열리지 않을 때**: Airflow standalone 초기 마이그레이션이 끝날 때까지 20~30초 기다린 뒤 `./scripts/lab.sh logs --tail 50 airflow`로 웹서버 기동 여부를 확인합니다.
- **예전에 Airflow를 켠 적이 있는 환경에서 웹 UI만 끝내 열리지 않을 때**: 이전 실행이 남긴 `/opt/airflow/airflow-webserver.pid`가 남아 새 웹서버가 시작되지 않을 수 있습니다(스터디 공통 환경 실측 2026-11-05: 웹서버 로그 `No response from gunicorn master within 120 seconds`). `./scripts/lab.sh exec -T airflow rm -f /opt/airflow/airflow-webserver.pid` 뒤 `./scripts/lab.sh --profile orchestration restart airflow`로 다시 띄웁니다.
- **자세한 안내**: [`operations.md`](operations.md#후속-회차--airflow), [`../labs/session06/student-guide.md`](../labs/session06/student-guide.md#airflow-30분-주기-스케줄과-멱등성-코드문서설계)

---

## 단계 8. `lesson07` Iceberg 유지보수와 `progress` 보존 실험 (7회차)

### ① 무엇을 확인하나
격리 실험판(`lake.lesson07`)에서 작은 커밋 120번이 만든 파일·스냅샷·메타데이터를 `rewrite_data_files` → `expire_snapshots` → `remove_orphan_files` → `rewrite_manifests`와 테이블 속성(`delete-after-commit`)으로 정리하고, 6회차 증분 시작점(`progress`) 만료 함정을 확인합니다(7회차).

### ② 명령
고정 데모 5세트(`s07-demo-grow`, `s07-demo-compact`, `s07-demo-expire`, `s07-demo-clean`, `s07-demo-trap`)를 생성한 뒤 Explorer 예제 8개를 확인하고, 이어서 개인 `RUN_ID`로 전체 유지보수 단계를 실행합니다.

```bash
# 데모 5세트 준비 (스터디 공통 환경 실측 2026-11-07 기준 약 90초 소요)
./labs/session07/demo.sh --fresh

# Explorer가 꺼져 있다면 기동
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --build --no-deps explorer
```

```bash
# 개인 RUN_ID로 전 단계(prepare→grow→compact→expire→orphans→manifests→progress-trap→verify) 실행
# (스터디 공통 환경 실측 2026-11-07 기준 약 27초 소요)
RUN_ID="s07-all-$(date +%Y%m%d-%H%M%S)"
./labs/session07/lab.sh all --run-id "$RUN_ID"
```

### ③ 화면 체크포인트
- **브라우저 Lakehouse Explorer (`http://localhost:28090` → SQL 탭 `7회차 · ...` 예제 8개 전체 체크포인트)**:
  1. `7회차 · grow snapshot·메타데이터 비교`:
     - `plain` `snapshots = 120`, `plain` `metadata_log = 101`
     - `tuned` `snapshots = 120`, `tuned` `metadata_log = 11`
  2. `7회차 · grow plain 최근 snapshot 5개`:
     - 5행 모두 `operation = 'append'`, `added_records = '1'`
  3. `7회차 · grow plain 데이터 파일 크기`:
     - `data_files = 120`, `avg_bytes ≈ 1227.8`(약 1.2 KB, 스터디 공통 환경 실측 2026-11-07)
  4. `7회차 · compact 현재 파일과 과거 파일`:
     - `plain`·`tuned` 모두 현재 `files = 1`, `all_data_files_distinct = 121` (압축 직후에는 이전 120개 스냅샷이 옛 파일 120개를 참조하므로 전체 고유 파일은 121개)
  5. `7회차 · expire 남은 snapshot과 파일`:
     - `plain`·`tuned` 모두 `snapshots = 1`, `all_data_files_distinct = 1` (스냅샷 만료 후 옛 데이터 파일 120개 삭제 완료)
  6. `7회차 · clean 현재·전체 매니페스트`:
     - `plain`·`tuned` 모두 현재 `manifests = 1`, `all_manifests_distinct = 23`
  7. `7회차 · trap inc 남은 snapshot`:
     - 위험한 만료 뒤 마지막 스냅샷 **1행**만 남음
  8. `7회차 · grow tuned 메타데이터 이력 · 최대 11개`:
     - `00111-...metadata.json`부터 `00121-...metadata.json`까지 **11행**만 유지됨
- **터미널 (`lab.sh all` 출력)**:
  - `compact`: `"rewritten_data_files_count": 120, "added_data_files_count": 1`
  - `expire`: `"deleted_data_files_count": 120, "deleted_manifest_files_count": 120, "deleted_manifest_lists_count": 120`
  - `manifests`: `"rewritten_manifests_count": 22, "added_manifests_count": 1`
  - `progress-trap`:
    - `"cleanup": "safe"` → **`"succeeded": true, "rows": 4, "passed": true`**
    - `"cleanup": "unsafe"` → **`"succeeded": false, "passed": true`**, `"error": "... Starting snapshot (exclusive) ... is not a parent ancestor of end snapshot ..."`
  - 마지막 줄 `"step": "verify"`: **`"match": true, "checked": 16`** (유지보수 전 과정에서 `rows: 120, checksum: 726000` 불변 확인)

### ④ 막히면
- **`No FileSystem for scheme "s3"` (`remove_orphan_files` 직접 호출 시)**: S3FileIO 환경이므로 프로시저 호출 시 반드시 `prefix_listing => true` 인자를 함께 넘겨야 합니다.
- **`Cannot remove orphan files with an interval less than 24 hours`**: Iceberg 1.10의 24시간 안전장치입니다. `older_than`을 현재 시각보다 최소 24시간 이전으로 지정합니다.
- **`이미 있는 데모 run-id: ...`**: `./labs/session07/demo.sh --fresh`를 실행합니다.
- **자세한 안내**: [`../labs/session07/student-guide.md`](../labs/session07/student-guide.md#8-집에서-따라-하기)

---

## 단계 9. 정리와 환경 끄기 (전체·7회차)

### ① 무엇을 확인하나
개인 실험으로 만든 격리 테이블과 선택 관찰 컨테이너를 정리하고, Flink savepoint를 남기며 환경을 안전하게 중지합니다.

> **경고 — 환경을 오래 켜 두면 데이터가 없어도 Bronze 메타데이터가 쌓입니다 (7회차 핵심 교훈)**:
> - [`../compose.yaml`](../compose.yaml)의 Flink 체크포인트 주기는 **`10s`(10초)**이고, Flink Iceberg Sink의 `flink.max-continuous-empty-commits` 기본값은 **`10`**입니다.
> - 따라서 원천 DB에 아무 변경이 없어도 **약 100초(10초 × 10회)마다 빈 스냅샷(empty commit)과 새 `metadata.json`이 생성**됩니다.
> - 공통 Bronze 테이블은 기본 속성(`write.metadata.delete-after-commit.enabled = false`)을 쓰므로, 며칠 동안 켜 두면 추적 목록(101개) 밖으로 밀려난 `metadata.json` 고아 파일이 수천 개씩 쌓여 디스크를 가득 채울 수 있습니다(스터디 공통 환경 실측 2026-11-05: 데이터 27행·8 KB에 메타데이터 2,953개·약 3.6 GB 누적, 세 Bronze 테이블 합계 약 10.8 GB).
> - **실습을 하지 않을 때는 반드시 `./scripts/stop.sh`로 환경을 멈춰 두세요.** 이미 오래 켜 두어 메타데이터가 쌓였다면 [7회차 학생 가이드의 유지보수 절차](../labs/session07/student-guide.md#7-운영-기준-체크리스트와-flink-테이블-주의점)를 참고해 정리합니다.

### ② 명령
6·7회차에서 개인 `RUN_ID`로 만든 테이블을 지우고 싶다면 `clean`을 실행합니다(수업 확인용 `s06-demo-*`·`s07-demo-*` 테이블을 강사에게 보여 줄 예정이라면 화면 공유 뒤에 정리합니다).

```bash
# (선택) 개인 실험 run-id 테이블만 DROP TABLE PURGE로 정리할 때
./labs/session06/lab.sh clean --run-id "$RUN_ID_S06"
./labs/session07/lab.sh clean --run-id "$RUN_ID_S07"
```

```bash
# (선택) 띄워 둔 관찰·거버넌스 컨테이너 정리
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml rm -sf explorer kafka-ui
./scripts/lab.sh -f compose.yaml -f compose.governance.yaml --profile governance stop om-server om-search om-db
```

```bash
# Airflow 중지 + Flink savepoint 생성 후 전체 컨테이너 안전 중지 (볼륨 데이터 유지)
./scripts/stop.sh
```

> **주의**: `docker compose down -v`는 원천 DB·카탈로그·Kafka·MinIO의 모든 실습 볼륨을 삭제하므로 일반 종료에 절대 사용하지 않습니다([`operations.md`](operations.md#시작중지)).

### ③ 화면 체크포인트
- **터미널 (`clean` 실행 시)**:
  - 6회차 `clean`: `"step": "clean"`, `"message": "이 run-id의 지정 테이블 7개만 DROP TABLE PURGE했어요."`
  - 7회차 `clean`: `"step": "clean"`, `"message": "이 run-id의 지정 테이블 3개만 DROP TABLE PURGE했어요."`
- **터미널 (`./scripts/stop.sh` 출력)**:
  - `flink_state.py stop`이 실행되어 `.runtime/flink-state.json`에 savepoint 경로가 기록되고 컨테이너들이 `Stopped` 상태로 내려갑니다.

### ④ 막히면
- **다음에 다시 켤 때**: 이미지가 이미 준비되어 있으므로 `./scripts/bootstrap.sh --no-build` 또는 `./scripts/self-study.sh --environment start`로 savepoint에서 이어서 기동합니다.
- **자세한 안내**: [`operations.md`](operations.md), [`../labs/session07/student-guide.md`](../labs/session07/student-guide.md#2-사고-보고-데이터-27행-메타데이터-36gb)

---

## 10. 강사에게 화면 공유할 때 보여 줄 것 (단계별 한 화면 요약)

화면 공유 시 상단 우측의 **`글자 크게 · 발표`** 버튼(Lakehouse Explorer)을 누르거나 터미널 글자 크기를 키우면 숫자를 빠르게 확인할 수 있습니다.

| 단계 | 회차 | 공유할 화면 (터미널 또는 브라우저 주소) | 강사가 바로 확인할 문구·숫자 |
|---|---|---|---|
| **1. 환경 기동** | 1~3회차 | Flink UI (`http://localhost:28081`) 또는 터미널 `preflight.py` | `cdc-bronze` `RUNNING`, `"ready": true`, `"checkpoint_failures_during_check": 0` |
| **2. 수집·Bronze** | 2~3회차 | 터미널 `artifacts/runs/self-study/<CASE_ID>/report.md` (또는 Kafka UI `:28082`) | `결과: **passed**`, 환불 3단계 원천 `1→1→0` / Kafka `1→2→4` / Bronze `1→2→3` |
| **3. Silver MERGE** | 4회차 | 터미널 `./labs/session04/lab.sh verify --run-id "$RUN_ID"` 출력 | `"passed": true`, `stored_rows` `["refund-r1", 30000, true, 300, 12]`, `current_business_row_count: 0` |
| **4. dbt·Gold** | 5회차 | 터미널 `./scripts/verify.sh` 마지막 줄 | 6개 케이스(`insert`~`old_update_after_delete`) 통과 후 `PASS: artifacts/runs/verification-...json` |
| **5. 거버넌스 (선택)** | 6회차 | 터미널 `contract-failure-demo.sh` 출력 (또는 OM `:28585` Lineage 탭) | `This model has an enforced contract that failed` 및 `amount \| BIGINT_TYPE \| STRING_TYPE` |
| **6. 증분·소급 정정** | 6회차 | Explorer (`http://localhost:28090`) `6회차 · s5 일별 Gold`, `s6 정상 일별 Gold`, `s6 naive와 전체 재계산 차이` | `s5` `2026-10-28`(`100000`)·`11-02`(`50000`) 2행, `s6` `2026-10-25`(`100000`)·`11-02`(`50000`) 2행, `naive` 차이 **1행**(`actual_minus_full`, `2026-10-28`) |
| **7. Airflow (선택)** | 6회차 | Airflow UI (`http://localhost:28080`) DAG 목록 | `lakehouse_refresh` **Paused(일시정지) 유지**, 스케줄 `*/5 * * * *`, import error 0건 |
| **8. 유지보수** | 7회차 | Explorer (`http://localhost:28090`) `7회차 · ...` 예제 및 터미널 `lab.sh all` 마지막 줄 | `grow` 스냅샷 `120`·메타데이터 이력 `101` vs `11`, `compact` 파일 `1` vs `121`, `expire` `1`, `clean` 매니페스트 `1`/`23`, `trap` 스냅샷 `1`, 터미널 `verify` `"match": true` |
| **9. 정리·중지** | 전체 | 터미널 `./scripts/stop.sh` 완료 후 `./scripts/lab.sh ps --all` | savepoint 저장 완료 및 실행 중인 컨테이너 없음(`Exited`) |
