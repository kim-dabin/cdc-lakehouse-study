# 1회차 강사용 실행 순서

모든 실행 명령은 **맥미니**에서 사용한다. 학생은 자기 노트북에서 SQL·설계서를 작성한다. 수업 시작 전에 서버를 준비하고, 진행 중에는 SQL 파일과 조회 화면을 공유한다.

## 수업 전 준비

### 전날 — 30~45분 별도 확보

1. 학생에게 비공개 저장소 읽기 권한 또는 학생 템플릿 파일을 제공한다. 신청 링크나 학생 계정은 이 저장소에 기록하지 않는다.
2. 맥미니 접속, 디스크 공간, 서비스와 복구 지점을 확인한다.
3. 아래 전체 시나리오를 새로운 CASE_ID로 실행하고 기록·화면 캡처를 저장한다.
4. 강사용 진행안과 학생용 안내를 각각 별도 창에 열어둔다.

```bash
ssh mini
cd ~/bootcamp/cdc-lakehouse-study
./scripts/lab.sh ps -a
```

정상 종료는 기존 `scripts/stop.sh`, 시작은 `scripts/bootstrap.sh`를 사용한다. 이전 작업이 사라졌고 복구 지점이 없다면 bootstrap이 재제출을 막는다. 이때 임의로 `.runtime/flink-state.json`을 삭제해 처음부터 재생하지 않는다. checkpoint 또는 savepoint 경로와 해당 테이블을 확인한 후 명시적으로 복원한다.

### 수업 20분 전

Airflow 자동 갱신을 멈춘다. 서버 이미지가 이미 준비된 환경에서 bootstrap과 readiness를 실행한다.

```bash
./scripts/lab.sh --profile orchestration stop airflow
./scripts/bootstrap.sh
./scripts/lab.sh exec -T runner python labs/session01/preflight.py
```

`ready: true`여야 한다. VM의 최소 여유 공간은 2GiB이며 5GiB 이상을 권장한다. 이미지를 새로 받거나 빌드해야 한다면 수업 전에 추가 공간과 시간을 확보한다. preflight의 PASS는 짧은 조회 준비 상태이며 2시간 연속 부하 검증 결과를 의미하지 않는다.

Flink UI를 보여줄 경우 **강사 노트북의 별도 터미널**에서 터널을 열어둔다.

```bash
ssh -N -L 28081:127.0.0.1:28081 mini
```

브라우저: `http://localhost:28081`. 온라인 수업의 공유 화면에는 `.env`, 로그인 값, 전체 커넥터 설정을 띄우지 않는다. 실습 SQL, Bronze 조회, 모델 SQL과 집계 결과를 보여준다.

## CASE_ID 정하기

**맥미니 터미널**에서 정한다. 매 리허설과 수업마다 새로운 값을 사용한다.

```bash
CASE_ID=s01-instructor-20261019a
```

위 날짜 문자열은 식별자 예시다. 실제 수업일을 확정하는 값이 아니다. `s01-`로 시작하고 소문자·숫자·하이픈만 사용한다. 이미 실행한 ID의 payment를 다시 실행하면 PK 오류가 나므로 새 CASE_ID를 사용한다. 기존 시연 행을 지워 초기화하지 않는다.

아래 도우미는 `labs/session01/sql/`의 PostgreSQL SQL을 실행한다. 원문 파일을 학생에게 보여준 뒤 실행한다. `:'namespace'`에는 CASE_ID가 바인딩되며 주문·결제·환불 키에 접미사가 붙는다.

## 1. 결제 100,000원

```bash
cat labs/session01/sql/01-payment.sql
./labs/session01/lab.sh "$CASE_ID" payment
./labs/session01/lab.sh "$CASE_ID" source
./labs/session01/lab.sh "$CASE_ID" wait --event payment
./labs/session01/lab.sh "$CASE_ID" bronze
./labs/session01/lab.sh "$CASE_ID" refresh
./labs/session01/lab.sh "$CASE_ID" check --expected 100000 --evidence
```

예상 결과:

- 원천 payments에 `CASE_ID-p1` 금액 100000.
- Bronze에 orders와 payments의 `op=c` 이벤트.
- Silver의 해당 결제 한 행, amount=100000.
- Gold의 `CASE_ID-o1`: paid=100000 / refunded=0 / net_paid=100000.
- check 출력의 `passed: true`.

wait는 이번 이벤트가 **Bronze에서 실제 조회될 때까지** 기다린다. checkpoint 횟수나 RUNNING만 보고 refresh를 먼저 실행하지 않는다.

## 2. 환불 20,000원 — 갱신 전후를 비교

```bash
cat labs/session01/sql/02-refund.sql
./labs/session01/lab.sh "$CASE_ID" refund
./labs/session01/lab.sh "$CASE_ID" wait --event refund
./labs/session01/lab.sh "$CASE_ID" bronze
./labs/session01/lab.sh "$CASE_ID" layers
```

**이때 refresh를 아직 하지 않는다.** Airflow가 멈춰 있으므로 source.net_paid=80000, gold_order의 net_paid=100000이어야 한다. source는 최신이고 Silver/Gold는 이전 상태인 이유를 학생에게 설명하게 한다.

```bash
./labs/session01/lab.sh "$CASE_ID" refresh
./labs/session01/lab.sh "$CASE_ID" check --expected 80000 --evidence
```

예상 결과: source와 Gold가 모두 80000. `silver_refund`의 amount=20000, is_deleted=false.

## 3. 같은 환불을 30,000원으로 정정

```bash
cat labs/session01/sql/03-correction.sql
./labs/session01/lab.sh "$CASE_ID" correction
./labs/session01/lab.sh "$CASE_ID" wait --event correction
./labs/session01/lab.sh "$CASE_ID" bronze
./labs/session01/lab.sh "$CASE_ID" refresh
./labs/session01/lab.sh "$CASE_ID" check --expected 70000 --evidence
```

예상 결과: Bronze 환불 c와 u 이력, u의 before_amount=20000 / after_amount=30000, Silver의 같은 환불 한 행 amount=30000, Gold net_paid=70000.

학생에게 “두 환불의 합 50,000원”이라는 답이 나오면 refund_id를 비교해 같은 행의 수정임을 다시 짚는다.

## 4. 원천 집계 답안 보여주기

학생이 먼저 원천 SELECT를 작성한 뒤 공개한다.

```bash
cat labs/session01/sql/expected-source-query.sql
./labs/session01/lab.sh "$CASE_ID" source-answer
```

기대값은 Gold와 같은 paid=100000 / refunded=30000 / net_paid=70000이다. 이 값이 다르면 Bronze 이후 처리 문제가 아니라 지표 정의 또는 원천 집계부터 확인한다.

## 5. 시간이 남으면 — 삭제 예고

핵심 120분 진행에 필수로 포함하지 않는다.

```bash
./labs/session01/lab.sh "$CASE_ID" delete
./labs/session01/lab.sh "$CASE_ID" wait --event delete
./labs/session01/lab.sh "$CASE_ID" bronze
./labs/session01/lab.sh "$CASE_ID" refresh
./labs/session01/lab.sh "$CASE_ID" check --expected 100000 --evidence
```

환불 행을 삭제했으므로 순결제액은 100000으로 돌아온다. 실무에서는 환불을 취소 상태로 변경할 수도 있으므로 삭제의 업무 의미를 먼저 정의해야 한다.

## 조회 출력 읽기

- `bronze`: topic / partition_id / kafka_offset / op / before_amount / after_amount / source_lsn / kafka_ts / ingested_at 순서다.
- `layers`: 원천 순결제액, Silver 결제·환불, Gold 주문 한 건을 이름별로 보여준다.
- `check`: 원천과 Gold의 금액을 기대값과 비교한다. Gold가 최신인지 확인한 이후에 사용한다.
- `--evidence`: `artifacts/runs/session01/CASE_ID/`에 기록한다. 새 리허설 기록을 강사 확인 후 자료에 포함한다.
- wait의 시간은 **대기 명령을 시작한 뒤 조회될 때까지**다. 원천 커밋부터의 end-to-end latency라고 설명하지 않는다.

## 장애 대응

| 증상 | 강사가 확인할 것 | 진행 방식 |
|---|---|---|
| PK 오류 | 같은 CASE_ID를 이미 썼는지 | 새 CASE_ID로 결제부터 진행 |
| FK 오류 | order → payment → refund 순서와 키 | 해당 SQL의 연결 키 검토 |
| Bronze 대기 시간 초과 | Connect 상태, Kafka 이벤트, Flink 작업과 checkpoint | 기다림을 반복하기 전에 어느 구간인지 확인 |
| Spark 연결 실패 | spark 서비스, 메모리 종료 기록, VM 디스크 | 강사가 복구, 학생은 원천 SQL 작성 계속 |
| 원천 8만원, Gold 10만원 | refresh 전인지 / Airflow가 중지됐는지 | 수업의 예상 중간 상태인지 설명 |
| 일별 마트 금액이 더 큼 | 이전 리허설 주문까지 집계됐는지 | 이번 order_id의 주문 마트로 검증 |
| 서버 문제 5분 이상 | 새로 실행을 증명할 수 있는지 | 리허설 JSON/캡처로 전환하고 이를 명시 |

로그 크기는 compose의 json-file 10MB × 최대 3파일 설정으로 제한한다. 이 설정은 컨테이너를 새로 만들 때 반영된다. Iceberg 이력과 이전 객체 버전은 별도의 보존 정책이 필요하므로 로그 제한만으로 저장소 증가가 해결되지는 않는다. 수업 준비 중 임의로 볼륨이나 checkpoint를 지우지 않는다.

## 수업 후

학생 파일과 설계 초안을 확인하고 다음 회차까지의 수정 사항을 정리한다. 다른 실습이 같은 VM에 있을 수 있으므로 VM 전체를 끄지 않는다. 이 저장소의 작업만 savepoint를 남겨 종료한다.

```bash
./scripts/stop.sh
```

수업 사이에 10초 주기 작업을 수 주간 계속 두지 않는다. savepoint 복원이 준비되어 있는지 종료 출력을 확인한다. Airflow는 이후 해당 회차에서 다시 준비하고 켠다.
