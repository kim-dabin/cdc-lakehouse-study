# 1회차 프로젝트 아키텍처

## 데이터 흐름

![데이터 흐름](../../docs/diagrams/01-data-flow.png)

## 저장·메타데이터 관계

![저장·메타데이터 관계](../../docs/diagrams/02-storage-metadata.png)

## 실행 환경

![실행 환경](../../docs/diagrams/03-runtime.png)

[그림 설명과 편집 가능한 draw.io 원본](../../docs/diagrams/README.md)

## 전체 데이터 흐름

```mermaid
flowchart TB
  student[학생 노트북<br/>공통 SQL · 이벤트 예측 작성] --> submission[개인 제출물<br/>SQL · 수집 대조 기록]
  teacher[강사 노트북<br/>온라인 화면 공유] -. SSH .-> pg
  repo[GitHub<br/>실습 코드 · 가이드] -. 코드 배포 .-> pg
  subgraph mini[Mac mini · Lima Linux ARM64 · Docker Compose]
    pg[(PostgreSQL<br/>orders / payments / refunds)] -->|WAL 변경| debezium[Debezium · Kafka Connect]
    debezium --> kafka[Kafka<br/>테이블별 변경 이벤트]
    kafka --> flink[Flink SQL<br/>원문 · 토픽 · 파티션 · 오프셋]
    flink -->|checkpoint 후 commit| bronze[(Iceberg Bronze<br/>cdc_events · 변경 이력)]
    bronze --> spark[Spark SQL<br/>최신 버전 판별 · MERGE]
    spark --> silver[(Iceberg Silver<br/>orders / payments / refunds<br/>현재 상태 · 삭제 표시)]
    silver --> dbt[dbt-spark → Spark Thrift<br/>모델 SQL · 의존성 · 테스트]
    dbt --> gold[(Iceberg Gold<br/>fct_order_settlement<br/>mart_daily_settlement)]
    gold --> result[후속 활용 테이블 조회]
    manual[후속 회차: Silver·Gold 갱신] -.-> spark
    airflow[이후 회차: Airflow 주기 실행] -.-> spark
    catalog[(별도 PostgreSQL<br/>Iceberg JDBC Catalog)] -. 공통 테이블 위치 .-> flink
    catalog -. 같은 카탈로그 .-> spark
    objectstore[(MinIO · S3 호환<br/>Parquet · Iceberg metadata)] --- bronze
    objectstore --- silver
    objectstore --- gold
    checkpoint[(영속 볼륨<br/>Flink checkpoint / savepoint)] --- flink
  end
```

1회차에는 미리 구성한 수집 경로에서 원천 INSERT·UPDATE·DELETE와 Kafka·Bronze 이벤트를 대조한다. 실행은 강사가 맡고 학생은 공통 예제의 기대 이벤트·원천 확인 SQL·수집 대조 기록을 작성한다. 개인 도메인·테이블 설계 확정은 3회차에 진행한다. Airflow는 중지하며 Silver·Gold는 후속 회차의 역할만 설명한다. 그림은 1회차 Bronze 검증과 후속 Silver·Gold를 구분한다.

## 예제 원천 모델

```mermaid
erDiagram
  ORDERS ||--o{ PAYMENTS : has
  PAYMENTS ||--o{ REFUNDS : has
  ORDERS {
    text order_id PK
    timestamptz ordered_at
    text status
  }
  PAYMENTS {
    text payment_id PK
    text order_id FK
    bigint amount
    text status
  }
  REFUNDS {
    text refund_id PK
    text payment_id FK
    bigint amount
    text status
  }
```

주문 1건 : 결제 N건, 결제 1건 : 환불 N건이다. 오늘은 주문·결제를 준비한 뒤 같은 refund_id의 입력·정정·삭제를 관찰한다. 원천 행 수와 Bronze 이력 행 수를 구분한다.

## 계층별 역할과 오늘의 범위

| 계층 | 저장 내용 | 이번 회차 |
|---|---|---|
| Bronze cdc_events | CDC 원문과 topic/partition/offset | c/u/d와 동일 수집 위치·내용 검증 |
| Silver | 키별 최신 상태·삭제 표시·원천 버전 | 역할 설명, 구현·검증은 후속 회차 |
| Gold | 활용 테이블과 테스트 | 역할 설명, 모델링은 후속 회차 |

Flink는 Bronze에 append 이력을 저장한다. 원천에서 삭제해도 c/u/d 이력은 남는다. Kafka null tombstone은 Flink 입력에서 제외하며 non-null op=d는 보관한다. source.lsn은 원천 버전 정보이고 Kafka 위치는 토픽·파티션·오프셋을 함께 사용한다. Kafka key는 Bronze 별도 컬럼으로 저장하지 않으며 원천 PK는 payload의 before/after에서 추출한다. 초기 스냅샷은 r, 새 변경은 c/u/d다.

## 공통 예제에서 확인할 질문

- 같은 refund_id의 금액이 20000에서 30000으로 바뀌면 원천 행과 Bronze 이력 행 수는 각각 어떻게 달라지는가?
- DELETE 뒤 after가 null일 때 원천 PK와 이전 금액은 어디에서 찾는가?
- Kafka의 어떤 위치 정보로 Bronze의 같은 이벤트를 대조하는가?
- null tombstone을 제외해도 실제 삭제 이력은 남는가?
- Kafka에 있고 Bronze에 없으면 어느 구간을 확인하는가?

[학생 SQL](sql/student-worksheet.sql)에서 예측하고 [제출 양식](submission-template.md) 4절에 실제 결과를 기록한다. 오늘은 별도 개인 아키텍처를 작성하지 않는다. 개인 도메인·테이블 설계는 3회차에 확정하며 후속 설계에는 [프로젝트 설계서](../../docs/project-template.md)를 사용한다.
