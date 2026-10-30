# 4회차 공식 참고 문서

확인일: 2026-10-26(실행 방식 선택 절은 2026-10-30). 실행 기준은 Spark 3.5.6·Iceberg 1.10.0입니다. 이번 수업의 키·최신 후보·MERGE·삭제·재실행·파일 비용을 다시 확인할 자료입니다. 모든 문서를 별도로 조사·제출하라는 목록은 아닙니다.

## 층의 역할

| 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| Bronze·Silver·Gold가 맡는 일 | [Databricks 메달리온 아키텍처](https://docs.databricks.com/aws/en/lakehouse/medallion) | Bronze의 원본 형식 유지·append·재처리, Silver의 정리·검증 작업 목록과 집계하지 않은 레코드, Gold의 집계·업무 규칙 |

## 먼저 볼 코드

| 자료 | 확인할 내용 |
|---|---|
| [4회차 가이드](student-guide.md) | 고정 입력과 수집 실험의 구분, 기대값·오류 확인 |
| [실행 진입점](lab.sh) · [실행기](lab.py) | 전용 run-id, 입력 검사, 단계별 SQL 제출·검증·누적 보고서 |
| [최신 후보 SQL](sql/01-latest.sql) | before/after·숫자형 LSN·PK별 ROW_NUMBER·삭제 포함 |
| [MERGE SQL](sql/02-merge.sql) | 같은 PK 매칭·새 버전 갱신·없는 키 삽입 |
| [확인 SQL](sql/03-check.sql) | 저장 행과 유효 업무 행, 키 유일성 |
| [기존 기준 구현](../../scripts/pipeline.py) | 전체 Bronze 재조회·세 테이블 순차 갱신·로컬 실행 조정 |
| [원천 DDL](../../sql/01-source.sql) | orders/payments/refunds 관계·REPLICA IDENTITY FULL |
| [기존 Flink 적재 SQL](../../flink/ingest.sql.template) | raw append와 Kafka wire-null 제외 |
| [환경 준비·기존 공통 실행](../../docs/self-study.md) | first/start/reuse 중 현재 상태에 맞는 실행 |

4회차 실행기는 `lake.lesson04`의 고정 fixture와 별도 Silver를 사용합니다. 기존 원천 수집이나 공통 마트 갱신을 수행한 결과로 해석하지 않습니다. 기존 `pipeline.py`와 4회차 실행기의 입력 검사 정책도 동일하다고 가정하지 않습니다.

## Spark SQL

| 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| 키별 최신 후보 | [Spark 3.5.6 Window Functions](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-window.html) | ROW_NUMBER, PARTITION BY, ORDER BY |
| JSON·타입 변환 | [Spark 3.5.6 Built-in Functions](https://spark.apache.org/docs/3.5.6/sql-ref-functions-builtin.html) · [ANSI Compliance](https://spark.apache.org/docs/3.5.6/sql-ref-ansi-compliance.html) | get_json_object와 변환 실패·NULL 처리 |
| 실제 물리 계획 | [Spark 3.5.6 EXPLAIN](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-explain.html) | 논리 규칙과 실제 실행 계획 구분 |
| 조인·shuffle·정렬 비용 | [Spark 3.5.6 Performance Tuning](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html) | 입력 크기·조인 전략·shuffle partition·AQE |

ROW_NUMBER가 최신의 업무 의미를 결정해주는 것은 아닙니다. 어떤 엔티티를 묶고 어떤 버전 순서가 비교 가능한지를 먼저 정해야 합니다. 실제 물리 단계도 설명 그림과 항상 일대일로 대응하지 않으며 계획과 실행 근거를 봅니다.

## Iceberg 쓰기·파일·commit

| 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| MERGE의 매칭과 쓰기 | [Iceberg 1.10 Spark Writes](https://iceberg.apache.org/docs/1.10.0/spark-writes/#merge-into) | 조건부 UPDATE·INSERT, 대상 한 행을 갱신할 입력 후보, 영향받는 파일 쓰기 |
| COW·MOR와 속성 | [Iceberg 1.10 Configuration](https://iceberg.apache.org/docs/1.10.0/configuration/) | `write.merge.mode`, copy-on-write·merge-on-read |
| 테이블 commit·동시성 | [Iceberg 1.10 Reliability](https://iceberg.apache.org/docs/1.10.0/reliability/) | snapshot·원자적 metadata 참조 변경·동시 쓰기와 재시도 |
| 파일·snapshot 확인 | [Iceberg 1.10 Spark Queries](https://iceberg.apache.org/docs/1.10.0/spark-queries/#inspecting-tables) | snapshots·files metadata tables |
| 반복 쓰기 이후 관리 | [Iceberg 1.10 Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/) | compaction·snapshot 만료·orphan 파일 정리의 다른 목적 |

이번 Silver는 COW를 명시합니다. 실제 반복 실행에서는 **업무 결과가 같아도 새 overwrite snapshot·파일이 생겼습니다.** 이 관찰은 공식 문서의 모든 버전·모든 계획에 대한 보장이 아닙니다. 실행 출력의 `before_storage`·`after_storage`를 비교하며 결과 멱등성과 물리 쓰기 비용을 나눕니다.

한 테이블의 commit을 orders·payments·refunds 전체의 원자적 갱신으로 해석하지 않습니다. 기존 기준 코드는 테이블마다 순차 실행하므로 중간 실패의 부분 갱신과 후속 작업 조건을 따로 설계해야 합니다.

## 원천 순서와 전달 위치의 범위

| 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| Debezium source 정보·삭제·snapshot | [Debezium 3.2 PostgreSQL Connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html) | source.lsn·op·before/after·delete와 tombstone |
| PostgreSQL 논리 디코딩 | [PostgreSQL 17 Logical Decoding](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html) | 변경 전달·slot·재전달 조건 |
| Kafka 파티션과 offset | [Kafka 3.9 Design](https://kafka.apache.org/39/design/design/) | 파티션 안 순서·소비 위치·보관 범위 |

현재 공통 예제는 단일 원천·고정 토픽·파티션 0에서 source.lsn 우선, 같은 LSN이면 offset을 비교합니다. 서로 다른 원천 LSN·파티션 offset을 전역 버전으로 비교하지 않습니다. 재스냅샷·원천 교체·토픽 재생성 등 버전 범위 변경도 별도로 판단합니다.

이번 fixture의 LSN 100/200/300과 단계별 상한은 설명용입니다. 실시간 CDC 도착·원천 commit 시각·운영 watermark·SLA의 관측값이 아닙니다.

## 실행 방식 선택 (설계 참고)

이번 실행기는 고정 입력에 MERGE를 단계별로 실행합니다. 아래 주기·스트리밍 방식 비교는 설계 참고이며, 수업에서 실행·측정한 결과가 아닙니다.

| 내용 | 공식 문서·소스 | 읽을 부분 |
|---|---|---|
| Silver를 이어 읽을 수 없는 이유 | [Iceberg 1.10 증분 읽기](https://iceberg.apache.org/docs/1.10.0/spark-queries/#incremental-read) · [Spark Structured Streaming](https://iceberg.apache.org/docs/1.10.0/spark-structured-streaming/) | append snapshot만 처리, overwrite는 기본 오류, commit 빈도와 trigger 간격 권고 |
| Silver 변경을 행 단위로 볼 때 | [create_changelog_view](https://iceberg.apache.org/docs/1.10.0/spark-procedures/#create_changelog_view) | snapshot 구간 변경, COW carry-over 행 제거 |
| 같은 MERGE를 micro-batch마다 | [Spark 3.5.6 foreachBatch](https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#foreachbatch) | 기본 at-least-once, batchId |
| Flink 키별 최신 행 | [Flink 1.20 Top-N](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/queries/topn/) · [Table 설정](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/config/) | ROW_NUMBER 기반 Top-N, `table.exec.state.ttl` 기본 0 |
| Flink에서 Iceberg upsert | [Iceberg 1.10 Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/#upsert) | v2 테이블, PK·identifier field, upsert 설정 |
| COW가 다시 쓰는 파일 | [Spark 3.5.6 RewriteMergeIntoTable](https://github.com/apache/spark/blob/v3.5.6/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/RewriteMergeIntoTable.scala) | 재작성 대상 파일을 ON 조건으로 선택 |
| dbt로 Silver를 만들 때 | [dbt-spark 1.9.3 배포 파일](https://pypi.org/project/dbt-spark/1.9.3/) · [merge 매크로 소스](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/materializations/incremental/strategies.sql#L24) | `dbt/include/spark/macros/materializations/incremental/strategies.sql`의 merge: ON·WHEN MATCHED |
