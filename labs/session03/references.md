# 3회차 공식 참고 문서

확인일: 2026-10-24. 본편은 Flink·Iceberg의 정의와 선택 이유, 실제 적재·commit·조회, 운영 판단을 다룹니다. 아래 문서는 필요한 부분을 다시 읽기 위한 자료이며 모두 별도 조사·제출해야 하는 목록은 아닙니다.

## 정의와 대안

| 설명할 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| Flink가 실행하는 일 | [Flink 1.20 Concepts](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/overview/) · [Architecture](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/concepts/flink-architecture/) | 처리 엔진·연산자·JobManager/TaskManager |
| Iceberg가 관리하는 일 | [Iceberg 1.10 Introduction](https://iceberg.apache.org/docs/1.10.0/) · [Reliability](https://iceberg.apache.org/docs/1.10.0/reliability/) | 테이블 형식, snapshot과 원자적 commit |
| 이미 배운 Spark로 처리하는 대안 | [Spark 3.5.6 Structured Streaming](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html) · [Iceberg Spark Streaming](https://iceberg.apache.org/docs/1.10.0/spark-structured-streaming/) | 기본 micro-batch 실행과 streaming append, experimental continuous 지원 경계 |
| 설정 중심 적재의 대안 | [Iceberg 1.10 Kafka Connect Sink](https://iceberg.apache.org/docs/1.10.0/kafka-connect/) | 입력 형식·commit 조정·라우팅과 설정 조건 |
| Kafka를 둘 이유와 한계 | [Kafka 3.9 Design](https://kafka.apache.org/39/design/design/) | 소비 위치·보관된 입력·전달 의미 |

대안은 성능 순위표가 아닙니다. 현재 raw append에 다른 엔진·connector를 쓸 수 있는지 비교하고 요구 지연·재처리·운영 비용으로 판단합니다. 실제로 대안을 설치하거나 동일 부하로 벤치마크했다는 뜻은 아닙니다.

## 실제 Flink SQL과 파일·commit 내부

| 설명할 내용 | 공식 문서·고정 릴리스 소스 | 읽을 부분 |
|---|---|---|
| raw 원문 읽기 | [Flink Raw Format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/raw/) | 단일 value 컬럼과 null 처리 |
| Kafka 전달 정보·시작 위치 | [Flink Kafka SQL Connector](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/) | Available Metadata, scan.startup.mode |
| METADATA/VIRTUAL | [Flink CREATE TABLE](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/create/#columns) | source metadata와 쓰기 스키마의 구분 |
| 복구할 위치·상태 | [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/checkpointing/) · [Kafka offset committing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/#consumer-offset-committing) | 재생 가능한 입력, 상태 복원과 group commit 위치의 차이 |
| 파일을 준비하는 writer | [Iceberg 1.10 IcebergStreamWriter](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.10.0/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergStreamWriter.java) | processElement, prepareSnapshotPreBarrier, flush |
| checkpoint 상태와 테이블 commit | [Iceberg 1.10 IcebergFilesCommitter](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.10.0/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java) · [Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/) | pending 결과·snapshotState·notifyCheckpointComplete·복원 시 대조 |
| 현재 sink 경로의 기본값 | [IcebergTableSink](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.10.0/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/IcebergTableSink.java) · [FlinkConfigOptions](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.10.0/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/FlinkConfigOptions.java) | 현재 코드에서 켜지 않은 experimental sink 경로와 구분 |
| JDBC Catalog의 현재 참조 | [JDBC Catalog](https://iceberg.apache.org/docs/1.10.0/jdbc/) · [JdbcTableOperations](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.10.0/core/src/main/java/org/apache/iceberg/jdbc/JdbcTableOperations.java) | metadata 파일 준비와 현재 위치의 조건부·원자적 갱신 |
| format v2 파일 관계 | [Iceberg 1.10 고정 명세](https://raw.githubusercontent.com/apache/iceberg/apache-iceberg-1.10.0/format/spec.md) | metadata JSON·snapshot·Avro manifest list/manifest·data/delete files |
| Debezium 삭제 사건과 tombstone | [Debezium 3.2 PostgreSQL Connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html) | 삭제 envelope와 tombstone 이벤트 |
| 원천 로그와 보관 조건 | [PostgreSQL 17 Logical Decoding](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html) | replication slot·변경 재전달·로그 보관 |

기준 SQL은 [flink/ingest.sql.template](../../flink/ingest.sql.template)입니다. raw value와 전달 metadata를 append하고 value 전체 null을 제외합니다. **삭제 envelope를 유지하는 이 Debezium 공통 예제에서** op=d는 보존되며, 일반 Kafka 토픽의 삭제 신호에도 같은 필터를 그대로 적용할 수 있다는 뜻은 아닙니다. JSON 유효성 검증·업무 키 upsert·업무 중복 제거를 이 SQL이 수행하지는 않습니다.

레코드는 writer 쓰기 경로로 들어가며 checkpoint 전까지 모두 메모리에만 대기하는 구조가 아닙니다. 내부 buffering·인코딩·업로드와 파일 마감 시점이 있으므로 레코드마다 즉시 완성 오브젝트가 보인다고도 하지 않습니다. checkpoint는 복구 상태, commit은 테이블 상태 변경이며 ID나 생성 횟수를 일대일로 연결하지 않습니다.

## MinIO 관찰과 운영 판단

| 설명할 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| snapshot·파일·metadata 이력 조회 | [Iceberg Spark Queries](https://iceberg.apache.org/docs/1.10.0/spark-queries/#inspecting-tables) | snapshots, files, metadata_log_entries, metadata column |
| checkpoint의 진행과 실패 | [Flink Checkpoint Monitoring](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/checkpoint_monitoring/) | 성공 시점·소요·실패·task 진행 |
| 하류 지연을 조사하는 backpressure | [Flink Back Pressure](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/back_pressure/) | busy·idle·backpressured time과 하류 관계 |
| writer/commit 관측 | [Iceberg Flink Metrics](https://iceberg.apache.org/docs/1.10.0/flink-writes/#metrics) | flush/commit 소요, 마지막 성공 commit 경과 시간 |
| 상태를 남긴 계획 종료 | [Flink Savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/savepoints/) | 복원 가능한 상태·호환성·보존 |
| 파일·snapshot·metadata 유지 관리 | [Iceberg Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/) · [Flink 쓰기 주의](https://iceberg.apache.org/docs/1.10.0/flink-writes/#notes) | compaction·snapshot 만료·orphan 정리의 다른 목적과 복구 참조 |

[관찰 SQL](metadata-inspection.sql)은 Spark 조회 A–D와 별도 Catalog PostgreSQL 조회 E를 구분합니다. MinIO 콘솔에서는 Catalog pointer → 현재 JSON → manifest 관계 → data 객체를 먼저 따라가고 SQL로 교차 확인합니다. 가장 최신 이름·시각만으로 current를 고르지 않습니다. files.record_count는 파일 전체 행 수이고, 여러 이벤트가 같은 _file에 들어갈 수 있습니다.

checkpoint·최근 snapshot·같은 키 SELECT는 서로 다른 순차 관측입니다. 원천 commit 시각을 확보하지 않은 wait 관찰을 전체 지연이나 고정 SLA로 표현하지 않습니다. 지표의 설명이 현재 helper에 모든 관측·알람이 구현됐다는 뜻도 아닙니다. 파일·metadata를 정리할 때는 진행 중 pending 파일과 Flink 복구에 필요한 참조를 함께 고려하며 보존 일수를 임의로 처방하지 않습니다.
