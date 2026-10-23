# 2회차 실습 가이드 — 같은 변경을 원천·메시지·테이블·파일에서 확인하기

**120분 · 강사 맥미니 시연 + 학생 SQL·예측·진행 공유**

지난 시간에는 같은 환불을 20,000원으로 입력하고 30,000원으로 정정한 뒤 삭제했습니다. 이번에는 Bronze에 실제로 남은 c/u/d 세 이력을 조회하고, Parquet 파일 안에 어떤 값이 저장됐는지 확인합니다. 이어서 PostgreSQL 수집 조건과 Debezium·Kafka 메시지가 이 결과에 어떻게 연결되는지 살펴봅니다.

수업 중에는 강사 화면을 보며 [SQL 학습지](sql/student-worksheet.sql)의 조회 조건과 기대값을 작성합니다. 수업 중에는 학생별 서버 기동이나 `submission.md` 작성을 진행하지 않습니다. 수업 후 복습에서는 **개인 환경 구성과 공통 예제 실행**을 진행합니다.

수업 중에는 [A] 조건 고치기, [D]의 DELETE 값 선택, c/u/d 예측, [E] 상황 하나 답하기에 집중합니다. [B]·[C]의 설정은 함께 읽고 전체 서술형 답은 복습에서 이어갑니다.

## 준비할 것

- 저장소의 최신 자료와 `labs/session02/sql/student-worksheet.sql`.
- SQL 편집기와 강사 공유 화면.
- 지난 복습에서 진행한 내용, 막힌 지점, 이번에 확인하고 싶은 질문.

SQL 학습지에는 **Spark SQL과 PostgreSQL이 함께** 있습니다. 해당 절에 표시된 엔진으로 나누어 읽고, 파일 전체를 한 번에 실행하지 않습니다. 아래 터미널 명령은 모두 저장소 루트에서 사용합니다. 수업 중에는 강사가 맥미니에서, 복습에서는 학생이 개인 환경에서 실행합니다.

## 120분 흐름

| 경과 시간 | 진행 | 학생이 할 일 |
|---|---|---|
| 00:00–00:10 | 진행 공유 | 실행한 것·막힌 지점·질문 나누기 |
| 00:10–00:30 | Bronze 세 이력·실제 Parquet 내용 | SQL [A]의 DELETE 조건을 보완하고 테이블·파일 비교 |
| 00:30–00:50 | PostgreSQL·Debezium 설정 | SQL [B]의 수집 조건과 [C]의 connector 설정 연결 |
| 00:50–00:55 | 휴식 | 5분 휴식 |
| 00:55–01:15 | 스냅샷·독립 Python 파서 | r 표본·fixture의 기대값과 SQL [D] 비교 |
| 01:15–01:35 | 새 키의 c/u/d 실시간 대조 | 먼저 예측하고 실제 Kafka·Bronze·parse 결과 확인 |
| 01:35–01:50 | 진단 | SQL [E]에서 먼저 확인할 구간과 증거 고르기 |
| 01:50–02:00 | 정리·복습 안내 | 확인한 것과 남은 질문 정리 |

## 1. 진행 상황 공유 — 00:00–00:10

지난 복습에서 어디까지 실행했는지, 어떤 화면이나 오류를 확인했는지 이야기합니다. 아직 실행되지 않는 환경은 마지막으로 확인한 단계와 질문을 나누면 됩니다. 개인 소개나 완성된 프로젝트 발표를 새로 준비하는 시간이 아니라, 구현 과정과 다음 확인할 일을 함께 짚는 시간입니다.

수업 중 메모는 필요한 부분만 남깁니다. 새 제출 양식을 채우기보다 이어지는 코드와 화면에 집중합니다.

## 2. Bronze 세 이력과 실제 파일 — 00:10–00:30

### 같은 환불을 찾는 조회부터 확인하기

강사가 지난 시연의 CASE_ID를 보여줍니다. 아래는 이월 결과를 읽는 예입니다. 이 키에 새 DML을 실행하지 않습니다.

```bash
CASE_ID=s01-live-10181152
./labs/session02/lab.sh "$CASE_ID" source-rows
./labs/session02/lab.sh "$CASE_ID" bronze --evidence
```

확인할 기대값은 다음과 같습니다. offset과 파일 경로는 실제 출력에서 읽으며 고정 숫자로 외우지 않습니다.

| 구간 | 같은 환불의 기대 결과 |
|---|---|
| 원천 | DELETE 뒤 해당 환불 0행 |
| Bronze c | before.amount=null, after.amount=20000 |
| Bronze u | before.amount=20000, after.amount=30000 |
| Bronze d | before.amount=30000, after=null |

`bronze` 출력의 `op`, `before_amount`, `after_amount`, `refund_id`, `topic`, `partition_id`, `kafka_offset`, `parquet_file`을 연결해 봅니다. **원천 현재 행과 변경 이력은 서로 다른 결과**입니다.

SQL [A]는 `after.refund_id`만 조건으로 사용합니다. 슬라이드 5를 보며 30–60초 동안 조건을 먼저 고친 뒤 슬라이드 6의 정답과 비교합니다. DELETE는 after가 null이므로 이 조건으로는 삭제 이력을 찾지 못합니다. `before.refund_id`도 포함하도록 WHERE 조건을 고쳐보세요. 조회 결과가 두 행이라고 해서 곧바로 수집 누락이라고 판단하지 않습니다.

### Parquet의 실제 내용 확인하기

```bash
./labs/session02/lab.sh "$CASE_ID" files --evidence
./labs/session02/lab.sh "$CASE_ID" parquet --evidence
```

`files`에서는 파일 경로·포맷·행 수·크기를 봅니다. `parquet`는 실제 저장소의 파일 bytes를 읽고 Parquet 형식과 디코딩된 행을 확인해 해당 환불 이력과 대조합니다. 경로만 출력한 것과 파일 내용을 읽은 것을 구분합니다.

파일 안에서 payload와 topic·partition_id·kafka_offset을 찾아 Bronze 조회 결과와 연결해 보세요. c/u/d 세 이벤트가 반드시 파일 하나에 들어가는 것은 아닙니다. DML 한 번과 파일 한 개, DML 한 번과 snapshot 한 개도 항상 일치하지 않습니다.

SQL [A]의 `lake.bronze.cdc_events.files`는 Iceberg 현재 테이블이 참조하는 파일을 읽는 메타데이터 조회입니다. MinIO 폴더의 모든 파일을 모은 것과 구분합니다. 파일 경로와 실제 내용을 함께 확인한 뒤 수집 결과를 판단합니다.

## 3. PostgreSQL 수집 조건과 Debezium 설정 — 00:30–00:50

```bash
./labs/session02/lab.sh source-config
```

원천 DB에서 수집 설정을 읽습니다. SQL [B]도 PostgreSQL용 조회입니다.

| 확인할 항목 | 이 예제에서 읽을 값 | 역할 |
|---|---|---|
| wal_level | logical | 논리 복제에 필요한 WAL 수준 |
| publication | cdc_lab, orders·payments·refunds | 발행할 테이블과 변경 종류 |
| replica identity | 예제 세 테이블 FULL | UPDATE·DELETE 이전 행 값 관찰 조건 |
| replication slot | cdc_lab, pgoutput | 소비 진행 위치와 WAL 보존에 관련된 상태 |

publication과 slot은 이름이 같아도 역할이 다릅니다. publication은 무엇을 발행할지, slot은 소비자의 진행 위치·WAL 보존과 관련된 상태를 다룹니다. `active=true`는 해당 slot을 사용하는 연결 상태를 보여주며 Iceberg 반영 완료를 뜻하지 않습니다.

실제 출력에서는 slot 상태의 `active`, `restart_lsn`, `confirmed_flush_lsn`을 봅니다. 슬라이드 14의 `wal_status`는 공식 뷰의 추가 참고 컬럼이며 오늘 helper 출력에는 없습니다.

예제는 `REPLICA IDENTITY FULL`을 사용하므로 이전 amount를 관찰합니다. 모든 PostgreSQL 기본 설정에서 before의 일반 컬럼이 같은 모습으로 들어오는 것은 아닙니다. SQL 출력의 replica identity 코드가 `f`라면 FULL을 뜻합니다.

SQL [B]의 질문에는 현재 설정과 역할만 적습니다. publication이나 slot을 지워 새로 만들 필요는 없습니다.

### Debezium 설정을 원천 설정과 연결하기

```bash
./labs/session02/lab.sh connector-sanitized
```

`connector-sanitized`는 수업에 필요한 설정을 비밀번호 없이 보여줍니다. 다음 연결을 SQL [C]에서 찾아보세요.

- `publication.name=cdc_lab`, `slot.name=cdc_lab`, `plugin.name=pgoutput`은 앞에서 읽은 PostgreSQL 설정과 연결됩니다.
- `table.include.list`에는 원천 세 테이블이 들어 있습니다. DB 발행 범위와 connector 수집 범위를 함께 확인합니다.
- `topic.prefix=cdc`와 원천 스키마·테이블 이름으로 `cdc.public.refunds`를 읽습니다.
- `snapshot.mode=initial`은 최초 데이터 스냅샷과 이후 변경을 이어받는 설정입니다. 재기동할 때마다 기존 복구 위치를 무시하고 전체 스냅샷을 새로 읽는다는 뜻은 아닙니다.

전체 connector 설정이나 `.env` 대신 필요한 항목을 추린 위 출력을 봅니다. 새 개념을 모두 다시 설명하기보다, 앞에서 본 결과가 어느 설정에 연결되는지 확인합니다.

## 4. 스냅샷 r과 독립 Python 파서 — 00:55–01:15

### 남아 있는 스냅샷 표본 읽기

```bash
./labs/session02/lab.sh snapshot-sample
```

`snapshot-sample`은 Kafka를 새로 소비하는 명령이 아니라 Bronze에 보존된 r 표본을 읽으며 `origin=live_bronze`로 표시합니다. `sample_found`를 확인하고, 표본이 없다면 미관찰로 구분한 뒤 명시된 synthetic fixture로 구조만 비교합니다. 기존 행을 처음 읽은 r과 새 환불의 c/u/d를 구분합니다. r을 보여주기 위해 커넥터·slot·데이터를 초기화하지 않습니다.

Kafka key는 식별 정보를, value는 Debezium 이벤트 본문을 담습니다. Bronze에는 value 원문과 수집 위치가 저장되며 key가 별도 컬럼으로 복제되지는 않습니다. 원천 PK는 payload 안의 before·after에서 찾습니다.

### 원문 한 건을 해석하는 함수

먼저 입력을 보고 어떤 결과가 나올지 예측한 뒤 실행합니다. 이 명령은 Python 3에서 실행하며 DB·Kafka 서버에 연결하지 않습니다. 수업에서는 강사가 실행하고 학생은 입력과 기대값을 비교합니다.

```bash
python3 labs/session02/event_parser.py --fixtures
```

`event_parser.py`의 `parse_event(raw_key, raw_value, metadata)`는 메시지 한 건을 입력받아 결과를 돌려주는 작은 함수입니다. `kind`로 change·tombstone·invalid를 구분합니다. change에는 `op`, `pk`, `pk_source`, `before`, `after`, 선택한 `row`, `source_lsn`이 들어 있습니다. 원문을 삭제하거나 Bronze를 갱신하는 함수가 아닙니다.

수업에서는 핵심 분기와 c·d·wire-null·JSON 텍스트 `null` 표본을 함께 읽고 나머지 검증 분기는 복습으로 이어갑니다. 슬라이드 19의 `raw_value`는 wire 입력이며 `value`는 JSON 해석 결과입니다. 실제 코드에서 `json.loads` 뒤 `value`가 None인 경우는 `invalid_json_null`이며 wire-null tombstone과 다릅니다.

| 입력 | 해석할 때 볼 것 |
|---|---|
| c | 새로 입력된 after |
| u | 변경 후 after와 필요할 때 비교할 before |
| d | 삭제 직전 before, after는 null |
| r | 스냅샷으로 읽은 after, op는 r로 구분 |
| tombstone | value 자체가 null인 메시지 |
| 손상된 JSON·잘못된 key·모르는 op | invalid와 error_code |

Kafka wire value가 없는 `None`과 문자열 `"null"`을 JSON으로 해석한 결과도 다릅니다. 전자는 tombstone으로, 후자는 `invalid_json_null`로 구분합니다. 파서가 오류를 조용히 정상 행으로 바꾸지 않는 이유를 생각해보세요.

SQL [D]는 같은 원리를 Spark SQL로 표현합니다. `COALESCE(after_id, before_id)`로 환불 ID를 찾고, DELETE이면 before.amount를 읽습니다. 여기서 만든 값은 이벤트 한 건의 해석이며, 환불별 최신 상태 테이블을 갱신한 결과가 아닙니다.

fixture 출력의 `origin=synthetic_fixture`를 확인합니다. 각 fixture는 독립 입력이며 실제 수업의 금액 변경 순서를 재생한 기록이 아닙니다. 고정 예제가 통과한 것과 실제 파이프라인에서 이벤트가 수집된 것은 따로 확인해야 합니다.

슬라이드 20의 실제 `parse` 조회는 다음 구간에서 새 CASE_ID를 만든 뒤 진행합니다.

## 5. 새 키로 예측한 뒤 실제 이벤트 대조 — 01:15–01:35

### 실행 전에 예상하기

강사가 새 CASE_ID를 만듭니다. 앞에서 읽은 이월 키와 구분합니다. helper가 기존 수집 예제와 연결되므로 `s01-`로 시작하는 형식을 사용합니다.

각 단계마다 실행 전에 원천 행 수·op·before/after·Bronze 이력 수를 예상해보세요. 환불 ID와 수집 위치를 어디에서 찾을지도 생각합니다. SQL [C]·[D]의 예측과 연결합니다.

강사 화면은 `--summary`로 짧게 보여줍니다. INSERT에서 전체 필드를 맞춘 뒤 UPDATE·DELETE는 달라진 값 중심으로 봅니다. 아래 복습 명령은 `--summary` 없이 원문과 `row` 객체까지 읽으며, INSERT·UPDATE의 추가 `collect-check`는 복습 보강입니다. 요약의 `event_amount`와 원문 파서의 `row.amount`, SQL [D]의 `event_amount`는 같은 이벤트의 선택된 금액으로 대조합니다.

```bash
CASE_ID=s01-s02-live-$(date +%m%d%H%M%S)
./labs/session02/lab.sh "$CASE_ID" payment
./labs/session02/lab.sh "$CASE_ID" wait --event payment --evidence
```

### INSERT — 환불 20,000원

```bash
./labs/session02/lab.sh "$CASE_ID" refund
./labs/session02/lab.sh "$CASE_ID" source-rows
./labs/session02/lab.sh "$CASE_ID" wait --event refund --evidence
./labs/session02/lab.sh "$CASE_ID" kafka
./labs/session02/lab.sh "$CASE_ID" collect-check --stage insert --summary --evidence
./labs/session02/lab.sh "$CASE_ID" bronze --evidence
./labs/session02/lab.sh "$CASE_ID" parse --evidence
```

원천 1행·20000, c의 before=null·after.amount=20000, 이번 키의 Bronze c 이력을 대조합니다. 실제 파서 결과는 `origin=live_kafka`로 표시됩니다. fixture의 답을 외운 것이 아니라 같은 원문을 실제로 해석했는지 봅니다.

### UPDATE — 같은 환불 30,000원 정정

```bash
./labs/session02/lab.sh "$CASE_ID" correction
./labs/session02/lab.sh "$CASE_ID" source-rows
./labs/session02/lab.sh "$CASE_ID" wait --event correction --evidence
./labs/session02/lab.sh "$CASE_ID" kafka
./labs/session02/lab.sh "$CASE_ID" collect-check --stage update --summary --evidence
./labs/session02/lab.sh "$CASE_ID" bronze --evidence
./labs/session02/lab.sh "$CASE_ID" parse --evidence
```

원천은 여전히 1행·30000이며 u의 before.amount=20000·after.amount=30000을 봅니다. Bronze에는 c와 u가 서로 다른 수집 위치의 이력으로 남습니다. parser의 선택한 row와 SQL [D]의 event_amount도 비교합니다.

### DELETE — 원천 행과 남은 이력

```bash
./labs/session02/lab.sh "$CASE_ID" delete
./labs/session02/lab.sh "$CASE_ID" source-rows
./labs/session02/lab.sh "$CASE_ID" wait --event delete --evidence
./labs/session02/lab.sh "$CASE_ID" kafka
./labs/session02/lab.sh "$CASE_ID" collect-check --stage delete --summary --evidence
./labs/session02/lab.sh "$CASE_ID" bronze --evidence
./labs/session02/lab.sh "$CASE_ID" parse --evidence
```

원천 0행, d의 before.amount=30000·after=null, 이번 키의 Bronze c/u/d 이력을 봅니다. Kafka의 내용 있는 op=d와 value=null tombstone을 구분합니다. 이 예제는 tombstone을 제외하고 non-null c/u/d와 r을 Bronze에 보관하므로 Kafka 메시지 수를 Bronze 행 수와 그대로 비교하지 않습니다.

수업에서는 요약으로 구분한 뒤 `kafka --evidence`로 원문을 한 번 더 조회합니다. 위 복습 명령의 `kafka`는 이미 원문 출력이므로 같은 조회를 다시 할 필요는 없습니다. op=d JSON 객체의 `after: null`과 tombstone의 `value: null`을 직접 비교합니다.

같은 Kafka 이벤트는 **topic·partition·offset을 함께** 사용해 대조합니다. offset 순서 비교는 같은 토픽·파티션 안에서 의미가 있고, PK나 source.lsn 하나를 전역 고유 이벤트 ID로 삼지 않습니다.

파서는 운영 중인 Flink와 별도로 원문을 해석합니다. Flink의 raw 적재 방식을 바꾸지 않습니다. 이번 키의 기대 결과를 확인했다고 해서 모든 원천 변경의 무누락·중복 방지나 장애 복구를 보장하는 것은 아닙니다.

## 6. 어느 구간부터 조사할까 — 01:35–01:50

SQL [E]에서 한 상황씩 읽고 먼저 확인할 곳을 골라봅니다.

강사 화면의 학습지 [E]를 보며 전체 채팅이나 구두로 한 상황에 답합니다. 3번은 before 조건을 포함하면 달라지는 결과를, 4번은 앞서 본 실제 파일 분포와 연결해 생각합니다.

| 관찰한 상황 | 먼저 확인할 것 |
|---|---|
| 원천은 바뀌었는데 Kafka에 해당 키가 없음 | 원천 commit, publication·include list, connector 상태와 읽은 키·토픽 |
| Kafka에는 있으나 Bronze에는 없음 | Flink 입력 토픽, 처리·checkpoint, Iceberg commit·저장소 접근, 실제 조회 |
| Bronze 조회에서 DELETE만 없음 | after에만 걸린 조건인지, before의 PK도 포함했는지 |
| 파일 하나에 c만 있음 | 다른 현재 파일, 같은 키의 전체 Bronze 조회, 선택한 파일 범위 |

컨테이너 RUNNING·slot active·checkpoint 완료는 각각 다른 상태입니다. 무엇이 확인됐고 무엇이 아직 확인되지 않았는지 말해보세요. 장애를 새로 만들어 해결하는 실험은 이번 진단 활동에 포함하지 않습니다.

## 7. 정리와 수업 후 복습 — 01:50–02:00

원천 설정 → Kafka 메시지 → Bronze 행 → 실제 파일에서 같은 변경을 어떻게 찾았는지 정리합니다. 개인 도메인·테이블 설계 확정은 3회차에 진행합니다. Silver·Gold 변환과 dbt 모델 구현은 후속 범위입니다.

### 개인 환경 준비

[README의 환경 요건](../../README.md)을 먼저 확인합니다. 현재 저장소는 Linux ARM 컨테이너를 실행할 수 있는 Docker 환경을 기준으로 합니다. 강사 맥미니에 접속하거나 강사 전용 설정을 복사하지 않습니다.

처음 clone한 폴더에 `.env`가 없다면 예제 파일을 복사합니다. 기존 `.env`가 있다면 덮어쓰지 않습니다.

```bash
cp .env.example .env
```

기존 환경은 예제 파일을 다시 복사하지 않고 아래 기동·점검으로 이어갑니다.

```bash
./scripts/bootstrap.sh
./scripts/lab.sh exec -T runner python labs/session01/preflight.py
```

최초 기동에는 기본 build가 필요합니다. `bootstrap.sh`는 준비 과정에서 Silver·Gold/dbt 작업도 실행하지만, 이번 복습에 이 계층의 모델 구현을 추가하는 것은 아닙니다. 기동 결과와 `ready` 상태를 확인한 뒤 공통 예제를 실행합니다.

### 개인 환경에서 같은 흐름 다시 확인하기

환경이 준비되면 **5절의 새 CASE_ID 설정부터** 자신의 환경에서 실행합니다. 강사 시연 키나 앞선 개인 실행 키를 재사용하지 않습니다. 각 변경 전에 예측하고 원천 → wait → Kafka → 대조 요약 → Bronze → parse를 읽습니다.

`source-config`·`connector-sanitized`로 설정을 확인하고, 같은 개인 CASE_ID의 `files`·`parquet`로 실제 파일도 읽습니다. 독립 fixture와 개인 Kafka 표본은 구분해 정리합니다.

실행 중 막히면 오류 화면·마지막으로 진행한 명령·기대했던 결과를 남기고 질문합니다. 기대값이나 강사 화면의 값을 개인 환경에서 실측한 값처럼 적지 않습니다.

### 복습에서 남길 것

- 개인 환경에서 확인한 설정과 공통 예제의 실행 결과·캡처.
- SQL 학습지 [A]–[E]의 SQL·예측·진단 답.
- 테이블과 실제 파일, Python 파서와 SQL의 연결, 남은 질문.

[복습 양식](submission-template.md)을 `submission.md`로 저장하고 `student-worksheet.sql`과 함께 제출합니다. 캡처 이미지도 함께 첨부해 다른 사람이 열 수 있게 하며, Markdown에 개인 컴퓨터의 경로만 남기지 않습니다. 제출 방법과 기한은 강사 안내를 따릅니다.

종료할 때는 자신의 프로젝트 상태를 저장하고 중지합니다.

```bash
./scripts/stop.sh
```

설정과 파일 조회의 근거를 더 읽으려면 [공식 참고 문서](references.md)를 봅니다.
