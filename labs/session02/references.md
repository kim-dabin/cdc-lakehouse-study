# 2회차 공식 참고 문서

2026-10-22 확인. 수업은 저장소에 고정된 PostgreSQL 17.6, Debezium 3.2.4.Final, Kafka 3.9.1, Flink 1.20.4, Iceberg 1.10.0, Spark 3.5.6을 기준으로 한다. 최신 버전으로 교체하는 수업이 아니다.

## PostgreSQL에서 무엇을 내보내는가

| 공식 문서 | 수업에서 확인할 내용 |
|---|---|
| [PostgreSQL 17 — Logical Decoding Concepts](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html) | WAL을 논리 변경으로 해석하는 과정, 연결과 별도로 유지되는 replication slot, WAL 보존 비용과 재전달 가능성 |
| [PostgreSQL 17 — CREATE PUBLICATION](https://www.postgresql.org/docs/17/sql-createpublication.html) | publication에 포함한 테이블과 내보낼 변경 종류. 수업에서는 생성·변경하지 않고 현재 구성을 조회한다. |
| [PostgreSQL 17 — ALTER TABLE / REPLICA IDENTITY](https://www.postgresql.org/docs/17/sql-altertable.html) | UPDATE·DELETE의 이전 행을 식별하기 위해 WAL에 남기는 정보. 예제의 FULL 설정과 이전 금액 관찰을 연결한다. |
| [PostgreSQL 17 — pg_replication_slots](https://www.postgresql.org/docs/17/view-pg-replication-slots.html) | active, restart_lsn, confirmed_flush_lsn, wal_status의 의미. LSN을 바이트 단위 위치로 보며 지연 시간으로 읽지 않는다. |

슬롯 상태를 보는 SELECT와 슬롯을 소비하거나 진행시키는 함수 호출은 다르다. 이번 실습은 상태 조회만 수행한다. 기존 슬롯에 새 소비자를 붙이거나 복구 위치를 바꾸지 않는다.

**2026-10-23 추가 확인 — SHOW와 WAL**

| 공식 문서 | 수업에서 확인할 내용 |
|---|---|
| [PostgreSQL 17 — SHOW](https://www.postgresql.org/docs/17/sql-show.html) | `SHOW wal_level;`은 현재 설정을 조회한다. 설정을 바꾸는 명령이 아니다. |
| [PostgreSQL 17 — Write-Ahead Logging](https://www.postgresql.org/docs/17/wal-intro.html) | WAL은 데이터 파일의 변경을 기록하기 전에 로그를 먼저 영구 저장해 복구에 사용하는 방식이다. CDC 전용 로그로 설명하지 않는다. |
| [PostgreSQL 17 — wal_level](https://www.postgresql.org/docs/17/runtime-config-wal.html#GUC-WAL-LEVEL) | `minimal`, `replica`, `logical`의 차이. `logical`은 replica 수준에 논리 디코딩에 필요한 정보를 더한다. 이 값 하나만으로 CDC의 모든 수집 조건이 충족되지는 않는다. |

**2026-10-23 추가 확인 — publication의 범위와 전달**

| 공식 문서 | 수업에서 확인할 내용 |
|---|---|
| [PostgreSQL 17 — Publication](https://www.postgresql.org/docs/17/logical-replication-publication.html) | 어떤 테이블의 어떤 변경을 발행할지 정의한다. publication 생성과 소비자의 연결은 구분한다. 이번 구성의 소비자는 Debezium이며 PostgreSQL subscription을 새로 만들지 않는다. |
| [PostgreSQL 17 — pg_publication_tables](https://www.postgresql.org/docs/17/view-pg-publication-tables.html) | publication과 포함된 테이블의 관계. 테이블 목록 조회는 발행 범위를 확인하는 것이며 실제 Kafka 전달의 증거는 아니다. |
| [PostgreSQL 17 — pg_publication](https://www.postgresql.org/docs/17/catalog-pg-publication.html) | `pubinsert`, `pubupdate`, `pubdelete`, `pubtruncate`로 발행할 변경 종류를 확인한다. 이 설정과 테이블 목록을 함께 읽는다. |

이번 경로는 Debezium이 지정된 publication과 slot을 사용해 PostgreSQL의 논리 복제 스트림을 읽는다. 테이블이 발행 범위에 있는지와 변경 종류가 켜져 있는지 확인한 뒤, connector의 연결·include list와 실제 Kafka 이벤트까지 대조한다. publication은 데이터를 보관하는 큐가 아니다.

## Debezium과 Kafka에서 어떤 이벤트를 보는가

| 공식 문서 | 수업에서 확인할 내용 |
|---|---|
| [Debezium 3.2 — PostgreSQL Connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html) | pgoutput, publication·slot·table.include.list, snapshot.mode=initial, r/c/u/d, before/after, DELETE와 tombstone |
| [Kafka 3.9 — Administration](https://kafka.apache.org/39/kafka-connect/administration/) | connector와 task의 상태 조회. RUNNING 상태만으로 특정 이벤트가 목적지에 도착했다고 판단하지 않는다. |
| [Kafka 3.9 — Protocol / Partitioning](https://kafka.apache.org/39/design/protocol/) | 토픽을 구성하는 파티션과 파티션 안의 순서. 서로 다른 파티션의 offset 숫자를 시간순으로 비교하지 않는다. |
| [Kafka 3.9 — Quick Start](https://kafka.apache.org/39/getting-started/quickstart/) | 이벤트를 읽는 소비자와 Kafka Connect의 역할. 기존 수집 소비자의 group offset을 변경하지 않는 별도 조회를 사용한다. |

**2026-10-23 추가 확인 — Debezium과 Kafka Connect의 역할**

| 공식 문서 | 수업에서 확인할 내용 |
|---|---|
| [Debezium 3.2 — Architecture](https://debezium.io/documentation/reference/3.2/architecture.html) | 이번 배포에서 Debezium source connector는 Kafka Connect 안에서 실행되고 PostgreSQL의 논리 복제 스트림을 읽는다. Connect 서비스와 Kafka broker를 구분한다. |
| [Kafka 3.9 — Kafka Connect User Guide](https://kafka.apache.org/39/kafka-connect/user-guide/) | worker·connector·task의 역할과 converter가 key/value를 Kafka에 저장할 형식으로 직렬화하는 과정. 이번 Python 파서는 수집 후의 독립적인 해석 도구다. |

`snapshot.mode=initial`은 보존된 진행 위치가 있는 정상 재시작 때마다 전체 스냅샷을 다시 뜬다는 뜻이 아니다. 이번 수업에서는 초기 스냅샷을 보여주기 위해 기존 connector나 offset을 삭제하지 않는다. `r` 표본이 현재 환경에 없으면 예시라는 점을 밝히고 수집된 c/u/d와 구분한다.

Kafka 메시지의 value 자체가 null인 tombstone과, DELETE 이벤트 객체 안의 `after: null`을 구분한다. 이 예제는 `tombstones.on.delete=true`이며 Flink SQL은 null value를 Bronze에서 제외한다. 이는 예제의 적재 선택이다.

**2026-10-23 추가 확인 — 강사 Kafka UI에서 key/value 보기**

| 공식 문서·버전 고정 소스 | 수업에서 확인할 내용 |
|---|---|
| [Kafbat UI — Misc configuration properties](https://ui.docs.kafbat.io/configuration/misc-configuration-properties) | `KAFKA_CLUSTERS_0_READONLY`의 읽기 전용 설정. 수업에서는 준비된 강사 화면을 함께 본다. |
| [Kafbat UI 1.5.0 릴리스](https://github.com/kafbat/kafka-ui/releases/tag/v1.5.0) | 수업에서 사용하는 UI 버전의 기준. 설정 문서는 갱신될 수 있으므로 아래 표시 방식은 1.5.0 태그의 소스와 구분해 확인한다. |
| [Kafbat UI 1.5.0 — MessageContent](https://github.com/kafbat/kafka-ui/blob/v1.5.0/frontend/src/components/Topics/Topic/Messages/MessageContent/MessageContent.tsx#L101-L105) | 본문 표시에서 비어 있는 값을 빈 문자열로 전달하는 부분. value 전체가 null이어도 화면에 글자 `null`이 항상 나타나는 것은 아니다. |
| [Kafbat UI 1.5.0 — BytesFormatted](https://github.com/kafbat/kafka-ui/blob/v1.5.0/frontend/src/components/common/BytesFormatted/utils.ts#L7-L9) | 크기가 없거나 0인 값을 `0 Bytes`로 표시하는 부분. 빈 값·크기 표시만으로 null과 빈 문자열을 구분하지 않는다. |

위 버전의 표시 코드에 따르면 tombstone과 빈 문자열이 화면에서 비슷하게 보일 수 있다. UI의 key와 같은 topic·partition·offset의 helper 원문을 맞춰 `value: null`인지 확인한다. UI의 빈 칸이나 `0 Bytes`만으로 tombstone이라고 단정하지 않는다.

## 테이블 조회와 실제 파일 내용을 어떻게 연결하는가

| 공식 문서 | 수업에서 확인할 내용 |
|---|---|
| [Iceberg 1.10 — Spark Queries](https://iceberg.apache.org/docs/1.10.0/spark-queries/) | 테이블 SQL 조회, 현재 파일을 나열하는 files/data_files 메타데이터 테이블, snapshot·manifest의 관계 |
| [Iceberg 1.10 — S3FileIO API](https://iceberg.apache.org/javadoc/1.10.0/org/apache/iceberg/aws/s3/S3FileIO.html) | 이번 helper가 S3 객체를 열고 실제 바이트를 읽는 API |
| [Iceberg 1.10 — GenericParquetReaders API](https://iceberg.apache.org/javadoc/1.10.0/org/apache/iceberg/data/parquet/GenericParquetReaders.html) | 이번 helper가 현재 테이블 스키마를 사용해 선택한 Parquet의 행을 디코드하는 API |
| [Apache Parquet — File Format](https://parquet.apache.org/docs/file-format/) | 이 예제 파일 앞뒤의 PAR1 magic과 파일 메타데이터 구조 |
| [Spark 3.5.6 — Parquet Files](https://spark.apache.org/docs/3.5.6/sql-data-sources-parquet.html) | 별도 참고: Spark에서 특정 Parquet를 읽는 방법. 이번 물리 파일 helper의 내부 실행 방식과는 구분한다. |
| [Iceberg 1.10 — Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/) | 스트리밍 append와 checkpoint 이후 commit 처리. checkpoint 완료 표시와 실제 새 테이블 조회를 구분한다. |

Parquet 직접 읽기는 선택한 물리 파일의 내용을 확인하는 실습이다. 일반적인 Iceberg 조회에서는 현재 snapshot이 참조하는 파일과 삭제 정보를 함께 해석한다. 객체 저장소의 모든 Parquet 파일을 무작정 합쳐 읽은 결과를 현재 Iceberg 테이블과 같다고 설명하지 않는다. c/u/d가 파일 하나에 모여 있다고 가정하지도 않는다.

이번 helper는 현재 append-only Bronze에서 고른 파일의 크기·형식·디코드 행 수와 payload·Kafka 위치를 SQL 결과와 대조한다. SHA-256과 PAR1 표시만으로 행 내용의 일치까지 증명하는 것은 아니다. 실제 수집 검증은 디코드한 행의 비교 결과까지 확인한다.

## 작은 이벤트 파서

[Python 3.11 — json](https://docs.python.org/3.11/library/json.html)은 JSON 문자열을 Python 값으로 해석하는 근거다. PK 선택, 지원하는 op, tombstone 분기, 잘못된 이벤트 처리 정책은 이번 예제 코드에서 정한 계약이다. Python 파서를 작성한다고 Kafka Connect plugin이나 Flink 실행 코드가 교체되는 것은 아니다.

## 근거와 실험 결과를 구분하기

- 공식 문서는 제품의 동작과 설정 의미를 설명한다. 특정 수업 키의 수집 성공은 실제 원천·Kafka·Bronze·Parquet 조회로 확인한다.
- PK는 업무 대상, LSN은 PostgreSQL 로그 위치, `(topic, partition, offset)`은 Kafka 수집 위치다. 이 셋을 같은 고유 키로 취급하지 않는다.
- 과거 리허설 자료를 사용할 때는 그 실행 시각과 키를 표시한다. 예시·과거 기록·현재 실측을 섞지 않는다.
- 이번 조회로 모든 장애에서의 복구나 전체 데이터의 exactly-once를 증명했다고 말하지 않는다.
