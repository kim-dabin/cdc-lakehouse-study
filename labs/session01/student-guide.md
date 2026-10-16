# 1회차 실습 가이드 — 변경 데이터 수집과 파이프라인 설계

서비스 DB의 예약 취소·상품 삭제·금액 정정을 필요한 곳에 전달하는 일을 다룹니다. [왜 CDC를 배우는지와 만들 수 있는 프로젝트](why-cdc-and-projects.md)를 읽고, 원천·Kafka·Iceberg Bronze의 같은 변경을 대조합니다.

강사는 맥미니에서 실행하고 화면을 공유합니다. 학생은 자기 노트북에서 SQL과 설계서를 작성합니다. Kafka·Spark·Docker·S3·Airflow 등의 정규 과정을 들은 수강생 대상이며, 이번 회차에 서버 설치나 SSH 접속을 하지 않습니다.

## 준비와 실행 역할

[학생 SQL](sql/student-worksheet.sql)과 [제출 템플릿](submission-template.md)을 개인 폴더에 복사하고, 템플릿은 `submission.md`로 저장합니다. 비공개 저장소 권한이 없으면 강사가 같은 파일을 배포합니다.

**학생 파일은 로컬 설계·예측용입니다.** 학생별 SQL을 수업 중 맥미니에 올려 실행하지 않습니다. 예제 키 `my-r1`을 그대로 사용해도 됩니다. 강사는 공통 시연 CASE_ID로 별도의 SQL을 실행하며, 학생은 화면의 키를 관찰표에 적습니다.

## 120분 흐름

| 경과 시간 | 진행 | 학생이 할 일 |
|---|---|---|
| 00:00–00:18 | CDC 필요성·직무 역량·프로젝트 | 관심 도메인 후보 메모 |
| 00:18–00:33 | 아키텍처 세 장 | 역할 표 3줄 작성 |
| 00:33–00:45 | 원천 모델·DML·사전 예측 | SQL [A]의 SELECT와 관찰표 기대값 작성 |
| 00:45–01:10 | c/u/d 수집 대조 시연 | 단계별 before/after·offset·copies 기록 |
| 01:10–01:15 | 휴식 | 필요하면 고정된 결과 화면 확인 |
| 01:15–01:30 | 누락·중복·지연 | DELETE 결과 한 세트와 측정 한계 작성 |
| 01:30–01:55 | 개인 설계 | 핵심 테이블 하나·DDL·기대 이벤트 |
| 01:55–02:00 | 점검과 제출 | 수업 중 필수 초안 제출 |

## 1. 수집 경로와 저장 내용

[아키텍처 세 장](architecture.md)을 보며 역할 표를 채웁니다.

- Debezium은 초기 스냅샷과 PostgreSQL WAL 변경을 Kafka로 전달합니다. 기존 행의 스냅샷은 `op=r`, 이후 새 입력·수정·삭제는 `c/u/d`로 구분합니다. 오늘은 스냅샷 이후 새로 만든 시연 키를 관찰합니다.
- Kafka에는 메시지 key와 value가 있습니다. Flink는 이 실습에서 **value 원문(payload)과 topic·partition·offset을 Bronze에 저장**합니다. Kafka key를 Bronze의 별도 컬럼으로 저장하지 않습니다.
- Bronze 행에서 원천 PK는 payload의 before/after에서 찾습니다. DELETE는 after=null이므로 before의 PK를 확인합니다.
- MinIO는 실제 데이터·metadata 파일, 별도 PostgreSQL Catalog는 테이블 위치 정보, Flink 상태 볼륨은 checkpoint/savepoint를 보관합니다.
- Spark의 Silver 현재 상태와 dbt·Spark의 Gold 활용 테이블은 후속 회차에서 구현합니다. 오늘은 Airflow를 중지하고 Bronze까지 대조합니다.

## 2. 시연 전에 작성할 것 — 12분

orders·payments는 준비 데이터입니다. 핵심 관찰 대상은 같은 refunds 한 행입니다. `refund_id`는 환불 PK이고 `payment_id`는 결제를 가리키는 FK입니다.

1. 학생 SQL [A]의 UPDATE와 DELETE 뒤에 같은 `my-r1`을 조회하는 SELECT를 작성합니다.
2. 제출 템플릿 2절의 기대 원천 행·before/after 열을 **미리** 채웁니다.
3. UPDATE가 같은 환불의 금액 정정인지, 새 환불 추가인지 구분합니다.

| 변경 | 기대 원천 조회 | CDC 기대값 |
|---|---|---|
| INSERT | 같은 PK 한 행, 20000 | c / before=null / after.amount=20000 |
| UPDATE | 같은 PK 한 행, 30000 | u / before.amount=20000 / after.amount=30000 |
| DELETE | 해당 PK 0행 | d / before.amount=30000 / after=null |

예제는 `REPLICA IDENTITY FULL`을 설정해 이전 amount를 관찰합니다. 모든 PostgreSQL 기본 설정에서 같은 before 값이 나온다는 뜻은 아닙니다. 개인 DDL에도 이번 관찰용 설정을 적습니다. [Debezium PostgreSQL 문서](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events).

## 3. 시연 중 작성할 것 — 25분

강사는 변경마다 원천·Kafka·Bronze를 대조하고 **요약 화면을 최소 60초 고정**합니다. 학생은 이때 실제 before/after, Kafka offset, bronze_copies만 기록합니다. 전체 JSON과 모든 타임스탬프를 받아쓰지 않습니다.

강사가 확인할 근거:

- Kafka key의 refund_id와 원천 PK가 같음.
- 기대 op·before/after와 Kafka·Bronze의 내용·source.lsn이 같음.
- 같은 (topic,partition,offset)의 Bronze 기록이 이번 새 키의 통제된 시연에서 하나씩 존재함 (`bronze_copies=1`).
- DELETE 후 원천은 0행, Bronze에는 c/u/d 이력이 남음.

Kafka에는 삭제 뒤 같은 key의 value=null tombstone이 있을 수 있습니다. 이 파이프라인은 null을 제외하고 non-null op=d를 Bronze에 남깁니다. 비교할 것은 Kafka 전체 건수 대신 non-null CDC 이벤트입니다. Kafka에 도착했어도 sink commit 전에는 Bronze에서 아직 안 보일 수 있습니다.

PK가 같아도 c/u/d는 정상 이력입니다. 동일 Kafka 위치가 반복 저장된 경우와 같은 내용이 다른 offset으로 재전달된 경우는 구분해서 조사합니다. offset 순서는 같은 토픽·파티션 안에서만 비교하고 source.lsn만을 전역 고유 이벤트 ID로 쓰지 않습니다.

## 4. 시연 뒤 작성할 것 — 15분

누락·중복 판단을 설명한 뒤 **DELETE 요약 한 세트만** 지연 표에 옮깁니다.

| 값 | 뜻 |
|---|---|
| kafka_ts | Kafka 레코드의 timestamp metadata |
| ingested_at | Flink에서 CURRENT_TIMESTAMP를 평가한 처리 시각 |
| wait_observed_seconds | wait 명령 시작 후 Bronze에서 처음 관찰할 때까지의 시간 |

`wait --evidence` 기록이 collect-check 요약의 wait_observation에 연결됩니다. 기록이 없으면 null이며 추정값을 만들지 않습니다.

이 값만으로 원천 commit부터 Iceberg 조회 가능 시점까지의 전체 지연을 계산할 수 없습니다. 이번 sink는 checkpoint 완료 후 Iceberg commit을 시도하며, 처리 시점·checkpoint 완료·commit·조회 간격에 따라 관찰 지연이 달라집니다. **최소 한 checkpoint 주기를 반드시 기다린다고 단정하지 않습니다.** checkpoint 성공만으로 sink commit 성공을 보장하지도 않습니다. [Iceberg Flink Writes](https://iceberg.apache.org/docs/latest/flink-writes/).

| 관찰 | 먼저 확인할 구간 |
|---|---|
| 원천만 바뀜 | commit, 수집 대상, connector |
| Kafka에 있고 Bronze에 없음 | 토픽 패턴, Flink 처리·checkpoint, sink commit·저장소 접근 |
| Bronze는 최신, Silver·Gold는 과거 값 | 후속 변환·갱신 여부 |

한 키의 c/u/d 대조와 EXACTLY_ONCE 설정 한 줄만으로 전체 무누락·중복 방지·복구를 보장했다고 쓰지 않습니다. 재전달·장애 복구 실험은 이후 회차에서 수행합니다.

## 5. 개인 설계 — 25분, 핵심 테이블 하나

처음 5분은 주제 선택과 작성법 안내입니다. 나머지 **20분은 체크리스트 화면을 고정**하고 각자 작성합니다.

예약 변경 이력, 물류 상태 동기화, 상품 정보 레이크, 구독 상태 관리, 결제·환불 정정 중 선택하거나 자신의 주제를 고릅니다. [주제별 최소 범위와 확장 실험](why-cdc-and-projects.md#프로젝트별-최소-완성-범위와-선택-확장)을 참고합니다.

### 수업 중 필수

- 주제와 CDC 선택 이유 한 문장.
- 핵심 변경 테이블 **하나**의 한 행 의미·PK·수정 컬럼·삭제 방식.
- 해당 DDL과 `ALTER TABLE ... REPLICA IDENTITY FULL;` 초안.
- 같은 PK의 c/u/d 기대 op와 before/after. 개인 DML 전체는 과제로 완성해도 됩니다.
- 원천부터 Bronze까지 자신의 테이블·토픽을 넣은 경로 한 줄. 나머지 테이블은 이름만 적습니다.

### 다음 회차 전 과제

나머지 테이블 1–2개의 명세·DDL·관계를 확장하고 개인 DML·원천 SELECT를 완성합니다. 수집 범위·목표 지연·보존과 publication/connector/Flink 연결 계획도 보완합니다. DDL만 만들면 새 테이블이 자동 수집되지는 않습니다.

## 제출

`student-worksheet.sql`과 `submission.md`를 강사가 안내한 채널에 제출합니다. 필수 초안과 미완성 과제를 구분해서 표시합니다. 프로젝트 발표는 8회 스터디 뒤 별도 일정입니다.
