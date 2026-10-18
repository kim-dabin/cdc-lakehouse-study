# 1회차 실습 가이드 — 공통 예제의 변경을 예측하고 수집 결과 확인하기

**120분 · 강사 맥미니 시연 + 학생 SQL 작성·수집 결과 대조**

주문·결제를 준비한 뒤 같은 환불 한 건을 입력하고, 금액을 정정하고, 삭제합니다. PostgreSQL의 현재 행과 Kafka 이벤트, Iceberg Bronze의 변경 이력이 어떻게 달라지는지 대조합니다.

Kafka·Spark·Docker·GitHub·S3·Airflow·Kubernetes 정규 과정을 수강한 학생 대상입니다. 이번 회차에는 공통 예제로 전체 흐름을 익힙니다. 개인 프로젝트의 도메인·테이블 설계 확정은 3회차에 진행합니다.

## 수업 전에 준비할 파일

1. [학생 SQL](sql/student-worksheet.sql)을 자신의 작업 폴더에 복사합니다.
2. SQL 편집기와 강사 공유 화면을 함께 볼 수 있게 준비합니다. 수업 중에는 이 SQL 파일 하나만 사용합니다.

강사는 맥미니에서 실행하고 학생은 로컬 파일을 작성합니다. 학생별 Docker 설치·기동, SSH 접속, 서버 SQL 실행은 필요하지 않습니다. 학생 SQL의 `my-r1`은 예측용 이름입니다. 실제 시연은 강사가 새 CASE_ID를 사용하므로 화면의 CASE_ID와 실제 환불 ID를 구분해서 봅니다.

## 120분 흐름

| 경과 시간 | 진행 | 학생이 할 일 |
|---|---|---|
| 00:00–00:12 | OT·8회 로드맵 | 학생 SQL 준비, 오늘의 확인 범위 확인 |
| 00:12–00:35 | 왜 CDC 레이크하우스인가 | CDC를 선택할 이유와 운영 비용 이해 |
| 00:35–01:05 | CDC·Iceberg 개념, 아키텍처 세 장 | 수집 경로·저장소·Catalog·복구 상태의 역할 구분 |
| 01:05–01:10 | 휴식 | 5분 휴식 |
| 01:10–01:35 | 저장소·실행 환경, ingest.sql 읽기, 사전 예측 | SQL [A] 코드 읽기·[B] SELECT 작성·[C] 이벤트 예측 |
| 01:35–01:55 | 환불 INSERT·UPDATE·DELETE 수집 대조 | 예측과 실제 before/after·offset·copies 대조, DELETE 지연 확인 |
| 01:55–02:00 | 정리·질문·2회차 안내 | 오늘 확인한 차이와 궁금한 점 나누기 |

## 1. 오늘 확인할 경로

[아키텍처 세 장](architecture.md)을 보며 다음을 구분합니다.

- **수집:** PostgreSQL → Debezium → Kafka → Flink → Iceberg Bronze.
- **저장·메타데이터:** MinIO는 데이터와 Iceberg 메타데이터 파일을 저장하고, JDBC Catalog는 현재 테이블 메타데이터의 위치를 관리합니다. Flink checkpoint·savepoint는 별도 상태 볼륨에 저장됩니다.
- **실행:** 강사는 맥미니의 Lima·Docker 환경에서 실행합니다. 학생은 공유 화면을 보고 자신의 예측과 실제 수집 결과를 대조합니다.
- **후속 변환:** Spark SQL Silver와 dbt Gold는 별도 작업이 실행될 때 갱신됩니다. 수업 전 준비 작업과 달리 오늘 c/u/d 시연에서는 refresh를 실행하지 않습니다. Airflow도 정지합니다.

Bronze·Silver·Gold는 데이터 처리 계층입니다. Iceberg 메타데이터 JSON → manifest list → manifest → 데이터 파일의 내부 구조와 구분합니다.

## 2. 코드를 읽고 결과 예측하기 — 01:10–01:35

강사의 컨테이너·preflight 점검을 함께 보고, 학생은 SQL 파일을 작성합니다. 핵심 상시 서비스는 9개이며 Spark와 runner는 별개입니다. 내부 포트와 맥미니 접속 포트는 [강사 실행 안내](runbook.md)에서 구분합니다.

### ingest.sql에서 확인할 네 곳

SQL [A]의 질문을 보며 각 설정의 역할을 주석으로 짧게 정리합니다.

1. `JdbcCatalog`·warehouse 설정: 테이블 메타데이터 위치와 실제 파일 저장소는 어디인가?
2. `topic-pattern`·`format='raw'`: 어떤 토픽의 무엇을 payload로 저장하는가?
3. checkpoint 설정: 데이터가 외부 조회에 보이려면 무엇이 추가로 성공해야 하는가?
4. `WHERE payload IS NOT NULL`: 어떤 메시지를 제외하고 무엇을 보관하는가?

### 같은 환불 한 건의 c/u/d 예측

orders·payments는 준비 데이터입니다. `refund_id`는 환불 PK이고 `payment_id`는 결제를 가리키는 FK입니다.

1. SQL [B]의 UPDATE·DELETE 뒤에 같은 `my-r1`을 조회하는 SELECT를 작성합니다.
2. SQL [C]에 c/u/d의 key·op·before/after를 먼저 예측합니다.
3. UPDATE가 새 환불 추가가 아니라 같은 환불의 금액 정정임을 확인합니다.

| 변경 | 기대 원천 조회 | CDC 기대값 |
|---|---|---|
| INSERT | 같은 PK 한 행, 20000 | c / before=null / after.amount=20000 |
| UPDATE | 같은 PK 한 행, 30000 | u / before.amount=20000 / after.amount=30000 |
| DELETE | 해당 PK 0행 | d / before.amount=30000 / after=null |

예제 원천 테이블에는 `REPLICA IDENTITY FULL`을 설정해 이전 amount를 관찰합니다. 모든 PostgreSQL 기본 설정에서 같은 before 값이 나온다는 뜻은 아닙니다. 기존 행의 초기 스냅샷은 `op=r`이고, 오늘 새로 만드는 환불은 c/u/d로 추적합니다.

## 3. 수집 결과 대조하기 — 01:35–01:55

강사는 **변경 실행 → 원천 조회 → wait → Kafka 이벤트 조회 → collect-check 요약** 순서로 진행합니다. 원천 변경 직후에는 Kafka 조회가 비어 있을 수 있어 도착을 기다린 뒤 보여줍니다. 각 단계 요약 화면을 최소 60초 유지합니다.

자신의 SQL [C] 예측과 화면의 실제 before/after·Kafka offset·bronze_copies를 대조합니다. 원천 행 수와 op·PK·수집 위치는 강사와 함께 확인합니다. 필요한 내용만 선택해서 메모하고, 전체 JSON을 받아쓸 필요는 없습니다. 요약의 `None`은 JSON의 `null`에 해당합니다.

| 확인할 것 | 판단 근거 |
|---|---|
| 같은 행을 추적하는가 | 원천 PK와 Kafka key의 refund_id 일치 |
| 변경 내용이 맞는가 | 기대 op·before/after와 Kafka·Bronze 내용 대조 |
| 같은 전달 위치가 저장됐는가 | topic·partition·offset 일치, 해당 위치의 bronze_copies=1 |
| 삭제 이력이 남는가 | 원천 0행, 이번 환불 키의 Bronze c/u/d 3건 |

Flink는 Kafka **value 원문과 수집 위치**를 Bronze에 저장합니다. Kafka key는 별도 Bronze 컬럼으로 저장하지 않으며 PK는 payload에서 찾습니다. DELETE는 after=null이므로 before.refund_id를 확인합니다. SQL [D]의 조회 예제도 before·after 양쪽을 조건에 포함합니다.

Kafka의 value=null tombstone은 제외하고, non-null c/u/d와 초기 스냅샷 r 이벤트는 보관합니다. tombstone을 포함한 Kafka 메시지 수를 Bronze 행 수와 그대로 비교하지 않습니다. **PK가 같아도 c/u/d는 서로 다른 정상 이력**입니다. 같은 Kafka 위치가 반복 저장됐는지와 다른 offset으로 재전달됐는지는 구분합니다. offset 순서는 같은 토픽·파티션 안에서 비교하고 source.lsn을 전역 고유 이벤트 ID로 사용하지 않습니다.

### DELETE 결과에서 지연 한 세트 확인

마지막 요약에서 아래 세 값의 의미와 기준 시점을 함께 확인합니다.

| 값 | 뜻 |
|---|---|
| kafka_ts | Kafka 레코드의 timestamp metadata |
| ingested_at | Flink에서 CURRENT_TIMESTAMP를 평가한 처리 시각 |
| wait_observed_seconds | wait 명령 시작 후 Bronze에서 처음 관찰할 때까지의 시간 |

`wait --evidence` 기록이 상세 대조 자료의 wait_observation에 연결되고 요약에는 관찰 시간이 표시됩니다. 관찰 기록이 없으면 값이 없는 이유를 확인하고 추정하지 않습니다.

checkpoint 완료 후 Iceberg commit을 시도하며, **commit 성공과 실제 조회**까지 확인해야 합니다. checkpoint 성공만으로 commit 성공을 보장하지 않고 DML과 스냅샷이 항상 1:1인 것도 아닙니다. 위 세 값만으로 원천 commit부터 분석 마트 반영까지의 전체 지연을 계산할 수는 없습니다.

| 관찰 | 먼저 확인할 구간 |
|---|---|
| 원천만 바뀜 | commit, 수집 대상, connector |
| Kafka에 있고 Bronze에 없음 | 토픽 패턴, Flink 처리·checkpoint, sink commit·저장소 접근 |
| Bronze는 최신, Silver·Gold는 과거 값 | 별도 변환·갱신 실행 여부 |

이번 새 키의 c/u/d 대조를 전체 데이터의 무누락·중복 방지·장애 복구 보장으로 확대하지 않습니다. 재전달·복구 실험은 후속 회차에서 진행합니다.

## 4. 정리와 다음 회차

SQL [A]에서 읽은 설정, [B]의 두 SELECT, [C]의 이벤트 예측을 다시 봅니다. 예측과 실제 결과에서 달랐던 점을 함께 짚고 질문을 나눕니다. SQL [D]는 제공된 조회를 읽고 DELETE 조건을 이해하는 참고용입니다.

수업 중 `submission.md` 작성이나 파일 제출은 진행하지 않습니다. 수집 결과 메모는 선택입니다. [수업 후 복습 양식 — 수업 중 사용 안 함](submission-template.md)은 수업 후 복습에 사용합니다. 제출 방법과 기한은 수업 마무리의 강사 안내를 따릅니다. 실제 시연 대신 이전 리허설 자료를 봤다면 과거 기록이라는 점을 구분하고, 확인하지 못한 값은 추정하지 않습니다.

2회차에는 공통 예제의 PostgreSQL → Debezium → Kafka 수집을 더 자세히 다룹니다. 오늘은 개인 DDL·개인 DML·지표 설계를 하지 않습니다. 개인 도메인·테이블 설계 확정은 3회차에 진행하며, 관심 도메인은 선택 메모만 남겨도 됩니다. 프로젝트 발표는 8회 스터디 뒤 별도 일정입니다.
