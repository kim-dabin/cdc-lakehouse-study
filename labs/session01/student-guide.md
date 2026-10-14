# 1회차 실습 가이드 — 변경 데이터 수집과 파이프라인 설계

이번 회차에서는 원천 DB의 한 행이 바뀔 때 Kafka에 어떤 메시지가 생기고, 그 메시지가 Iceberg에 어떻게 남는지 확인합니다. 변경을 식별하는 키, 수집 범위, 이력 저장 방식을 정해 개인 프로젝트의 파이프라인 초안을 작성합니다.

강사는 맥미니에서 미리 구성한 환경을 실행하고 화면을 공유합니다. 학생은 자기 노트북에서 SQL과 설계서를 작성합니다. 서버 접속이나 Docker·Flink 설치는 이번 실습에 포함하지 않습니다. Kafka·Spark 등 정규 과정에서 배운 내용을 바탕으로 진행합니다.

## 준비할 파일

[GitHub 실습 폴더](https://github.com/kim-dabin/cdc-lakehouse-study/tree/main/labs/session01)에서 아래 두 파일을 개인 폴더로 복사합니다. 비공개 저장소 접근 권한이 없다면 강사가 제공한 동일 파일을 사용합니다.

- [student-worksheet.sql](sql/student-worksheet.sql): DML, 원천 조회 SQL, 개인 DDL 작성.
- [submission-template.md](submission-template.md): `submission.md`로 저장해 수집 검증과 설계 기록.

SQL은 텍스트 편집기나 IDE에서 작성하면 됩니다. 학생 파일을 서버에서 자동 실행하지 않습니다. 강사가 답안을 검토하고 전용 시연 SQL을 실행합니다. 파일의 테스트 날짜는 실제 강의 일정과 관계없습니다.

## 120분 진행 순서

| 경과 시간 | 진행 | 학생이 할 일 |
|---|---|---|
| 00:00–00:10 | 프로젝트 목표와 수집 요구사항 | 어떤 변경을 수집할지, 성공 근거 작성 |
| 00:10–00:30 | 아키텍처 세 장과 구성 요소별 책임 | 수집 경로와 저장소·실행 환경 구분 |
| 00:30–00:45 | 원천 모델과 변경 시나리오 | PK/FK, 원천 조회, INSERT·UPDATE·DELETE 작성 |
| 00:45–01:10 | 세 변경의 Kafka·Bronze 기록 관찰 | 키, op, before/after, 수집 위치 대조 |
| 01:10–01:15 | 휴식 | |
| 01:15–01:30 | 수집 검증과 지연 확인 | 누락·중복 판단과 조사할 구간 작성 |
| 01:30–01:55 | 개인 파이프라인 설계 | 명세, 경로, DDL 하나, 검증 시나리오 |
| 01:55–02:00 | 점검과 제출 | SQL·설계 초안 제출 |

## 1. 전체 경로와 각자의 책임

[아키텍처 세 장](architecture.md)을 보며 설계서의 역할 표를 채웁니다.

- PostgreSQL: 원천 테이블과 커밋된 변경.
- Debezium/Kafka Connect: 초기 스냅샷과 WAL 기반 변경 이벤트 전달.
- Kafka: 테이블별 이벤트를 토픽·파티션·오프셋으로 기록.
- Flink: Kafka의 CDC 원문과 수집 위치를 Iceberg Bronze에 적재.
- Iceberg Bronze: 입력·수정·삭제 이벤트의 이력 보관.
- Spark SQL Silver: 이후 회차에서 키별 현재 상태와 삭제 표시 구성.
- dbt·Spark Gold: 이후 회차에서 활용할 테이블과 테스트 구성.

MinIO는 실제 파일을 저장하고, 별도 PostgreSQL의 Iceberg JDBC Catalog는 테이블의 metadata 파일 위치 등을 관리합니다. Flink checkpoint/savepoint는 별도 상태 볼륨에 남습니다. 오늘 Airflow는 멈춰두며 Silver·Gold 갱신을 수집 성공의 기준으로 사용하지 않습니다.

## 2. 같은 행에 세 가지 변경 만들기

예제의 `orders → payments → refunds` 관계를 확인합니다. `refund_id`는 환불 한 건의 PK이고 `payment_id`는 어떤 결제를 환불하는지 가리킵니다. 환불 한 행에 아래 순서를 적용합니다.

```sql
INSERT INTO refunds (refund_id, payment_id, amount, status)
VALUES ('my-r1', 'my-p1', 20000, 'success');

SELECT refund_id, payment_id, amount, status
FROM refunds WHERE refund_id = 'my-r1';

UPDATE refunds SET amount = 30000 WHERE refund_id = 'my-r1';

DELETE FROM refunds WHERE refund_id = 'my-r1';
```

실행 전 주문과 결제 행이 필요합니다. 학생 SQL 파일의 준비 INSERT를 먼저 읽고 FK가 일치하도록 자기 키로 바꿉니다. 위 SQL은 학생 작성 예시이며 강사 화면에서는 매번 새로운 CASE_ID를 사용합니다. 각 단계 직후 원천 SELECT의 기대 결과를 먼저 적습니다.

| 변경 | 원천 조회의 기대 결과 | CDC 기대값 |
|---|---|---|
| INSERT | 같은 PK 한 행, amount=20000 | op=c, before=null, after.amount=20000 |
| UPDATE | 같은 PK 한 행, amount=30000 | op=u, before.amount=20000, after.amount=30000 |
| DELETE | 해당 PK 조회 결과 0행 | op=d, before.amount=30000, after=null |

예제 원천은 `REPLICA IDENTITY FULL`을 설정해 수정·삭제 이전의 컬럼 값을 관찰합니다. 다른 설정에서는 before에 제공하는 필드가 달라질 수 있습니다. DELETE는 원천 행을 지우지만 Bronze에 쌓인 c/u/d 이력을 지우지 않습니다.

## 3. 강사 화면에서 Kafka와 Bronze 대조

각 단계마다 **원천 SELECT → Kafka 메시지 → Bronze 조회** 순서로 봅니다. 갱신을 기다리는 동안 아래 표의 예상값과 실제값을 구분해 기록합니다.

| 단계 | 원천 행 수·금액 | Kafka key·op·before/after | Kafka 토픽·파티션·오프셋 | 동일 위치의 Bronze 값 | 판정·근거 |
|---|---|---|---|---|---|
| INSERT | | | | | |
| UPDATE | | | | | |
| DELETE | | | | | |

확인할 내용:

1. Kafka key의 refund_id와 원천 PK가 같은가?
2. c/u/d와 before/after가 실제 변경에 맞는가?
3. Kafka와 Bronze의 **(topic, partition, offset)**이 같은가?
4. 동일 위치의 이벤트 내용과 source.lsn이 같은가?
5. 원천에서 삭제한 뒤에도 Bronze에 c/u/d 이력이 남는가?

새 CASE_ID의 환불만 필터링합니다. 예제에서는 Kafka의 non-null CDC 이벤트가 c/u/d 세 개이며 Bronze에도 해당 위치의 세 기록을 기대합니다. 작업 재시도나 동일 이벤트 재전달이 있다면 개수만 맞추지 말고 어떤 위치와 내용이 반복됐는지 적습니다.

Kafka offset은 파티션 안에서 비교합니다. 다른 파티션이나 토픽 사이의 전체 순서를 뜻하지 않습니다. source.lsn은 원천 변경 위치이며 단독으로 모든 이벤트의 고유 ID라고 가정하지 않습니다.

삭제 뒤 같은 key에 value=null인 **tombstone**이 보일 수 있습니다. 이 파이프라인은 non-null `op=d` 이벤트를 Bronze에 보관하고 null tombstone은 걸러냅니다. 그래서 Kafka 전체 메시지 수와 Bronze 행 수가 반드시 같지는 않습니다. 비교 대상은 non-null CDC 이벤트입니다.

## 4. 수집 지연과 이상 징후 판단

원천 변경 시각, Kafka 확인 시각, Bronze 첫 조회 시각을 기록합니다. 화면에서 `wait_observed_seconds`가 나오면 **대기 명령을 시작한 뒤 Bronze에서 처음 관찰할 때까지의 시간**으로 적습니다. 원천 커밋부터의 정확한 지연과는 다릅니다.

Bronze의 `kafka_ts`는 Kafka record 시각, `ingested_at`은 Flink 처리 시각입니다. `ingested_at - kafka_ts`만으로 Iceberg commit 후 조회 가능 시점까지의 전체 지연을 계산하지 않습니다. 정밀 계측은 이후 회차에서 같은 기준 시계와 commit 시점 기록을 추가합니다.

| 관찰한 상태 | 먼저 확인할 구간 |
|---|---|
| 원천은 바뀌었는데 Kafka 메시지가 없음 | 원천 커밋, publication·수집 대상, connector 상태 |
| Kafka에는 있는데 Bronze에서 안 보임 | 토픽 패턴, Flink 소비·처리, checkpoint와 sink commit, Catalog·저장소 접근 |
| 같은 PK의 이벤트가 여러 개 있음 | 정상 c/u/d 이력인지, 동일 위치 중복인지, 다른 위치의 재전달인지 구분 |
| Bronze에는 있으나 Silver·Gold가 과거 값 | 후속 변환·갱신 여부를 확인. 오늘 수집 검증과 별도 |

Flink 작업이 RUNNING이라는 사실과 이벤트가 실제 저장됐다는 사실을 구분합니다. 오늘 한 키의 세 변경을 확인한 결과만으로 모든 데이터의 무누락, 장애 복구, exactly-once를 보장했다고 쓰지 않습니다.

## 5. 개인 프로젝트의 파이프라인 설계

예약, 물류, 구독 등 각자 도메인을 고릅니다. 아래 순서로 25분 동안 작성합니다.

### 처음 5분: 수집 목적과 범위

- 데이터 소비자가 이 데이터를 어디에 쓸지 한 문장.
- 원천 테이블 2–3개와 포함·제외할 데이터.
- 원천 변경부터 Bronze 조회까지 목표 지연과 이유.
- 원천을 얼마나 오래 보관하며 이력 보존이 왜 필요한지.

예: 예약 변경 이력을 추적하고 취소 상태를 최신화한다. reservations·customers를 수집하고 개인정보 컬럼의 포함 여부를 적는다. 전체 고객 서비스를 한 번에 설계하지 않습니다.

### 다음 8분: 원천 명세와 경로

| 테이블 | 한 행의 의미 | PK/FK | 수정·삭제 컬럼 | 예상 토픽 | Bronze에 남길 정보 |
|---|---|---|---|---|---|
| | | | | | |
| | | | | | |
| | | | | | |

원천, connector, Kafka, Flink, Bronze를 연결하고 각 단계의 입력·출력을 적습니다. 저장소·Catalog·복구 상태 위치는 데이터 경로와 구분합니다. 예제 테이블 이름을 자기 도메인으로 바꿔 [설계 틀](architecture.md#개인-프로젝트-작성-틀)을 사용합니다.

새 테이블은 DDL만 작성하면 자동 수집되지 않습니다. publication, connector include list, Flink 토픽 패턴과 권한을 맞춰야 합니다. 1회차에서는 이 연결 계획을 적고 설정은 다음 회차에서 확인합니다.

### 다음 7분: DDL 하나와 변경 테스트

수정 가능한 테이블 하나의 DDL과 INSERT·UPDATE·DELETE를 작성합니다. 각 단계에서 기대하는 원천 행, Kafka key/op, Bronze 이력을 적습니다. 테이블 한 행의 PK를 바꾸지 않고 내용이 변하는 시나리오부터 설계합니다.

### 마지막 5분: 검증과 장애 조사 계획

- 어떤 key와 수집 위치로 누락을 찾을까?
- 같은 이벤트를 다시 처리하면 어떻게 식별할까?
- DELETE와 논리 삭제(status 변경)를 어떻게 구분할까?
- Kafka에 있는데 Bronze에 없으면 어느 구간부터 확인할까?

재처리 구현과 복구 실험은 이후 회차에서 수행합니다. 지금은 판단 기준과 필요한 기록을 설계서에 남깁니다.

## 제출과 다음 회차

오늘은 `student-worksheet.sql`과 `submission.md`를 강사가 안내한 채널에 제출합니다. 완료 기준은 c/u/d의 원천·Kafka·Bronze 대조 기록, 원천 명세, 개인 파이프라인 초안과 DDL 하나입니다.

다음 회차 전 나머지 DDL과 DML, 기대 이벤트 표를 완성합니다. 미확정 설정은 추정값으로 확정하지 않고 질문으로 남깁니다. 8회까지가 스터디이며 프로젝트 발표는 별도 일정입니다.

참고: [Debezium PostgreSQL 이벤트·replica identity·tombstone 문서](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events).
