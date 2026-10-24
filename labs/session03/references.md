# 3회차 공식 참고 문서

2026-10-23 확인, Flink 입력·출력과 Iceberg 기본 구조·commit 설명은 2026-10-24 추가 확인. 기술을 선택한 이유와 결과를 판단하는 경계를 더 읽어볼 자료입니다. 수업의 상세 설정·명령 목록을 늘리기 위한 자료는 아닙니다.

| 설명할 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| PostgreSQL의 변경을 논리적으로 읽기 | [PostgreSQL 17 Logical Decoding](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html) | 변경 전달과 replication slot의 역할 |
| 스냅샷과 변경 이벤트 | [Debezium 3.2 PostgreSQL Connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html) | 스냅샷·변경·삭제 이벤트 |
| Kafka의 소비 위치와 다시 읽기 | [Kafka 3.9 Design](https://kafka.apache.org/39/design/design/) | Consumer Position, Message Delivery Semantics |
| Kafka value를 원문으로 받기 | [Flink 1.20 Raw Format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/raw/) | raw value와 단일 컬럼, null 처리 |
| Kafka 전달 정보와 시작 위치 | [Flink 1.20 Kafka SQL Connector](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/) | Available Metadata, `scan.startup.mode` |
| `METADATA FROM`과 `VIRTUAL` | [Flink 1.20 CREATE TABLE](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/create/) | Metadata Columns와 읽기·쓰기 스키마 구분 |
| 처리 상태를 복구하는 checkpoint | [Flink 1.20 Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/checkpointing/) | 재생 가능한 입력과 영속 상태의 조건 |
| Catalog와 파일 저장의 역할 구분 | [Iceberg 1.10 JDBC Catalog](https://iceberg.apache.org/docs/1.10.0/jdbc/) | 관계형 DB를 이용한 Catalog와 원자적 테이블 커밋의 조건 |
| Flink에서 Iceberg에 쓰기 | [Iceberg 1.10 Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/) | checkpoint와 commit, commit 관측 지표 |
| 테이블 상태를 조회로 확인하기 | [Iceberg 1.10 Spark Queries](https://iceberg.apache.org/docs/1.10.0/spark-queries/) | snapshots·history 메타데이터 조회 |
| 기본 구조·snapshot·테이블 commit | [Iceberg 1.10 Reliability](https://iceberg.apache.org/docs/1.10.0/reliability/) | 현재 metadata 참조의 원자적 갱신과 일관된 snapshot 읽기 |

[실제 Flink SQL](../../flink/ingest.sql.template)을 기준으로 raw `payload STRING`과 전달 메타데이터를 읽습니다. `METADATA FROM`으로 Kafka 정보를 얻고, `VIRTUAL`은 원본 테이블의 쓰기 대상에서 해당 컬럼을 제외합니다. SELECT에 명시한 메타데이터는 Bronze의 일반 컬럼으로 보존합니다. `earliest-offset`은 복원할 상태가 없는 시작의 기준이며 checkpoint 복원은 저장된 처리 위치를 사용합니다.

이 예제의 Bronze는 raw CDC 원문을 append하고 value 전체가 null인 tombstone을 제외합니다. JSON 본문이 있는 `op=d`는 `after=null`이어도 남으며, 업무 키별 upsert를 수행하지 않습니다. 현재 상태를 고르는 책임은 후속 Silver에 있습니다. 개별 checkpoint 완료, 테이블 snapshot, 같은 키의 실제 SELECT는 서로 다른 관찰이며 번호를 일대일로 맞추지 않습니다.

3회차에서는 Catalog·Metadata·Data의 큰 관계와 파일 준비·commit·조회 경계까지만 다룹니다. snapshot은 테이블의 일관된 상태이며 전체 데이터 복사를 뜻하지 않습니다. Bronze `op=d` 행과 Iceberg Delete File도 구분합니다. Manifest·CAS·CoW/MoR·파일 관리와 튜닝의 세부는 후속 회차 범위입니다.

학습지의 두 지연은 프로젝트 목표입니다. helper wait의 관찰 시간이나 예제 설정을 고정 SLA로 표현하지 않습니다. 이번 수업은 복구 조건을 이해하는 단계이며 장애 주입·간격 변경 실험은 후속 범위입니다.
