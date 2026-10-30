-- Input has already been checked, and contains exactly one row per refund_id.
-- Keep a delete marker AND its version so older updates cannot resurrect it.
MERGE INTO {silver} AS t
USING {incoming} AS s
ON t.refund_id = s.refund_id
WHEN MATCHED AND (
  s.source_lsn > t.source_lsn
  OR (s.source_lsn = t.source_lsn AND s.kafka_offset > t.kafka_offset)
)
THEN UPDATE SET
  t.payment_id = s.payment_id, t.amount = s.amount, t.status = s.status,
  t.is_deleted = s.is_deleted, t.source_lsn = s.source_lsn,
  t.topic = s.topic, t.partition_id = s.partition_id,
  t.kafka_offset = s.kafka_offset
WHEN NOT MATCHED THEN INSERT (
  refund_id, payment_id, amount, status, is_deleted,
  source_lsn, topic, partition_id, kafka_offset
) VALUES (
  s.refund_id, s.payment_id, s.amount, s.status, s.is_deleted,
  s.source_lsn, s.topic, s.partition_id, s.kafka_offset
)
