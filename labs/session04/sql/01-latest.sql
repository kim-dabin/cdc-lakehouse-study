-- Spark SQL / Iceberg 1.10.0. Replace braces only with this run's safe table names.
-- This lesson replays fixed Bronze-shaped inputs; it does not collect new CDC.
-- source_lsn is a numeric source version, not a globally comparable clock.
CREATE OR REPLACE TEMP VIEW {incoming} AS
WITH envelopes AS (
  SELECT topic, partition_id, kafka_offset,
         get_json_object(payload, '$.op') AS op,
         CAST(get_json_object(payload, '$.source.lsn') AS BIGINT) AS source_lsn,
         CASE WHEN get_json_object(payload, '$.op') = 'd'
              THEN get_json_object(payload, '$.before')
              ELSE get_json_object(payload, '$.after') END AS row_json
  FROM {bronze}
), typed AS (
  SELECT get_json_object(row_json, '$.refund_id') AS refund_id,
         get_json_object(row_json, '$.payment_id') AS payment_id,
         CAST(get_json_object(row_json, '$.amount') AS BIGINT) AS amount,
         get_json_object(row_json, '$.status') AS status,
         op = 'd' AS is_deleted, source_lsn, topic, partition_id, kafka_offset
  FROM envelopes
  WHERE source_lsn <= {upper_lsn}
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY refund_id
    ORDER BY source_lsn DESC, kafka_offset DESC
  ) AS rn
  FROM typed
)
SELECT refund_id, payment_id, amount, status, is_deleted,
       source_lsn, topic, partition_id, kafka_offset
FROM ranked WHERE rn = 1
