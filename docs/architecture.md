# 전체 아키텍처

```mermaid
flowchart TB
  author[강사·수강생 노트북<br/>Git · SQL · 코드 작성] --> github[GitHub<br/>실습 코드 · 개인 프로젝트]
  author -. SSH / 포트 터널 .-> mini
  github -->|코드 배포| mini
  subgraph mini[Mac mini · ARM64 · Lima Linux VM · Docker Compose]
    direction TB
    pg[(PostgreSQL 원천 DB<br/>주문 · 결제 · 환불)] -->|WAL / pgoutput| cdc[Debezium · Kafka Connect<br/>초기 스냅샷 + 변경 이벤트]
    cdc --> kafka[Kafka<br/>테이블별 토픽 · 변경 로그 보관]
    kafka --> flink[Flink SQL<br/>원문 + 토픽·파티션·오프셋 기록]
    flink -->|체크포인트 후 Iceberg 커밋| bronze[Bronze · Iceberg<br/>추가 전용 CDC 이력]
    bronze --> spark[Spark SQL<br/>키·LSN 기준 최신 상태 · MERGE]
    spark --> silver[Silver · Iceberg<br/>현재 상태 + 삭제 표시 + 버전]
    silver --> dbt[dbt-spark → Spark Thrift<br/>모델 의존성 · SQL · 품질 테스트]
    dbt --> gold[Gold · Iceberg<br/>주문별·일별 순결제액 마트]
    gold --> query[SQL 조회 · 변경 반영 시간 확인]
    airflow[Airflow · 5분 주기<br/>Silver → dbt build] -. 내부 실행 API .-> spark
    airflow -. 내부 실행 API .-> dbt
    catalog[(별도 PostgreSQL<br/>Iceberg JDBC Catalog)] -. 테이블 메타데이터 위치 .-> flink
    catalog -. 같은 카탈로그 .-> spark
    storage[(MinIO · S3 호환<br/>Iceberg metadata + Parquet)] --- bronze
    storage --- silver
    storage --- gold
    checkpoint[(공유 영속 볼륨<br/>Flink checkpoint · savepoint)] --- flink
  end
```

## 각 계층에서 배우는 것

| 계층 | 저장하는 내용 | 확인할 질문 |
|---|---|---|
| PostgreSQL | 서비스의 현재 트랜잭션 상태 | 어떤 키가 한 건을 식별하는가? |
| Kafka | INSERT/UPDATE/DELETE 이벤트 | 어느 토픽·파티션·오프셋까지 전달됐는가? |
| Bronze | 원문 CDC와 전달 메타데이터 | 수정 전후와 삭제 사건을 다시 읽을 수 있는가? |
| Silver | 키별 최신 상태, 삭제 표시, LSN | 과거 이벤트가 다시 와도 현재 상태가 되돌아가지 않는가? |
| Gold | 주문일 기준 순결제액 | 환불 수정·삭제 후 기대한 금액과 일치하는가? |

**실시간 적재와 마트 갱신은 서로 다른 주기다.** Flink의 체크포인트 간격은 기본 10초이고, Iceberg에서 조회 가능한 시점은 실제 커밋 완료 이후다. Gold는 Airflow의 기본 5분 주기에 Spark SQL·dbt 처리 시간이 더해진다. 두 지연을 별도로 측정한다. 목표 지연은 각 프로젝트에서 정한다.

## 선택한 구성의 이유와 경계

- Kafka와 Spark는 정규 과정에서 배운 기술을 활용한다. 새로운 학습의 중심은 CDC 해석, Iceberg 테이블, 변경 반영, dbt 모델링과 검증이다.
- Bronze는 원문을 남긴다. 이 실습은 Flink가 Iceberg에 현재 상태를 직접 upsert하는 구조가 아니다. 현재 상태를 만드는 책임은 Spark SQL에 있다.
- JDBC Catalog와 MinIO는 ARM 맥미니 한 대에서 실행할 수 있는 구성이다. 실제 S3로 옮길 때는 엔드포인트·IAM·네트워크 설정을 바꿔야 한다.
- Spark SQL과 dbt는 같은 Spark Thrift Server와 Iceberg 카탈로그를 사용한다. dbt 자체가 연산 엔진은 아니다.
- 기준 구현은 Bronze 전체 조회 → Silver MERGE → Gold 전체 재생성이다. 먼저 결과의 정확성을 확보한 뒤 증분 범위와 마트 갱신 주기를 개선한다.
- 토픽당 파티션 하나, 원천 DB 하나를 전제로 LSN과 오프셋으로 순서를 판별한다. 토픽 재생성, DB 복원, 다중 파티션 확장은 별도 설계가 필요하다.
- 단일 노드 수업 환경이다. TaskManager 복구를 실습하며, Mac mini나 JobManager 전체 장애에 대한 HA를 제공하지 않는다. checkpoint 볼륨을 삭제하거나 Kafka 보관 기간이 지나면 복구 조건이 달라진다.

## 코드와 실행 환경

노트북에서는 코드를 작성하고 GitHub에 올린다. 컨테이너와 데이터는 맥미니에서 실행한다. UI는 SSH 터널로 접속하며 외부에 공개하지 않는다. 원천 DB·Kafka·객체 저장소·카탈로그·체크포인트는 각각 전용 볼륨을 사용한다.

여러 원천 테이블을 하나의 트랜잭션으로 동시에 갱신하는 경우, 이 기준 구현은 테이블 간 동일 트랜잭션 시점의 스냅샷을 보장하지 않는다. Bronze가 따라잡은 뒤 최종 결과가 수렴하는 것을 검증한다. 거래 단위의 즉시 일관성이 필요한 도메인은 트랜잭션 경계·완료 신호와 일관된 처리 범위를 추가로 설계한다.
