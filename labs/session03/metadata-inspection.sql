-- 3회차: MinIO 객체와 Iceberg 테이블 상태를 연결하는 읽기 전용 SQL
-- A~D: Spark SQL / Iceberg Bronze 연결. 원천 PostgreSQL에서 실행하지 않습니다.
-- E: 별도 Catalog PostgreSQL 연결. Spark 연결과 원천 app DB에서는 실행하지 않습니다.
-- 파일 전체를 한 번에 실행하지 말고 화면에 맞는 SELECT 한 개씩 선택합니다.
-- D의 <REFUND_ID> 두 곳을 같은 환불 ID로 교체합니다. ID를 새로 만들라는 뜻은 아닙니다.
-- 아래 LIMIT는 화면용 표본이며 전체 파일/행 수나 현재 pointer를 결정하지 않습니다.

-- [A] Spark SQL: 보존된 snapshot 표본
-- committed_at이 가장 최신인 snapshot이 항상 current라는 뜻은 아닙니다.
-- checkpoint ID와 snapshot ID를 일대일로 대응시키지 않습니다.
SELECT snapshot_id, committed_at, operation
FROM lake.bronze.cdc_events.snapshots
ORDER BY committed_at DESC
LIMIT 3;

-- [B] Spark SQL: 현재 테이블 파일의 표본과 MinIO data 경로 대조
-- record_count는 파일 전체 행 수이며 특정 환불 키의 이력 수가 아닙니다.
-- metadata 조회이므로 Parquet bytes를 직접 디코드한 증거는 아닙니다.
SELECT file_path, record_count, file_size_in_bytes
FROM lake.bronze.cdc_events.files
ORDER BY file_path
LIMIT 3;

-- [C] Spark SQL: metadata 변경 이력과 JSON 경로
-- 시간 정렬만으로 Catalog의 현재 metadata_location을 대신하지 않습니다.
SELECT timestamp, file, latest_snapshot_id
FROM lake.bronze.cdc_events.metadata_log_entries
ORDER BY timestamp DESC
LIMIT 2;

-- [D] Spark SQL: 같은 환불 키의 이벤트와 실제 data 파일 위치 연결
-- 삭제 뒤 after에는 키가 없으므로 before/after 양쪽에서 찾습니다.
-- 현재 공통 refunds 토픽의 단일 파티션 전제입니다. offset은 전역 순서가 아닙니다.
-- 여러 이벤트가 같은 _file에 들어갈 수 있습니다. 이벤트 한 건=파일 한 개가 아닙니다.
-- 결과가 늦어지면 파일 경로는 기존 files/parquet helper의 준비 증거로 대조합니다.
SELECT topic, partition_id, kafka_offset,
       get_json_object(payload, '$.op') AS op,
       _file
FROM lake.bronze.cdc_events
WHERE get_json_object(payload, '$.before.refund_id') = '<REFUND_ID>'
   OR get_json_object(payload, '$.after.refund_id') = '<REFUND_ID>'
ORDER BY kafka_offset
LIMIT 4;

-- [E] 별도 Catalog PostgreSQL만: 현재 JSON 위치
-- 현행 helper도 사용하는 읽기 조회입니다. Catalog 내부 테이블을 수정하지 않습니다.
-- 여러 catalog가 같은 DB에 있다면 catalog_name도 구분해야 합니다.
-- 같은 조건에 여러 행이 나오면 임의로 하나를 current라고 고르지 않습니다.
SELECT metadata_location
FROM iceberg_tables
WHERE table_namespace = 'bronze'
  AND table_name = 'cdc_events';
