-- Spark SQL / lake.bronze.cdc_events. 원천 PostgreSQL에서 실행하지 않습니다.
-- 이 파일은 조회 템플릿이며 psql 입력 파일이 아닙니다.
-- SQL 편집기에서 실행하려면 __REFUND_ID__ 두 곳 모두 자신의 CASE_ID + '-r1'로
-- 바꿉니다. 바깥 SQL 작은따옴표는 유지합니다.
-- 예시: s01-s03-my-1025170000-r1. 이 예시 ID를 그대로 복사하지 않습니다.
-- 변수 검증을 포함한 터미널 복사 명령은 README 4절에 있습니다.
SELECT topic, partition_id, kafka_offset,
       get_json_object(payload, '$.op') AS op,
       get_json_object(payload, '$.before.refund_id') AS before_refund_id,
       get_json_object(payload, '$.after.refund_id') AS after_refund_id,
       get_json_object(payload, '$.before.amount') AS before_amount,
       get_json_object(payload, '$.after.amount') AS after_amount,
       _file AS parquet_file
FROM lake.bronze.cdc_events
WHERE topic = 'cdc.public.refunds'
  AND (get_json_object(payload, '$.before.refund_id') = '__REFUND_ID__'
       OR get_json_object(payload, '$.after.refund_id') = '__REFUND_ID__')
ORDER BY partition_id, kafka_offset;
