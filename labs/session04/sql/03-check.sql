-- The stored row retains amount=30000 and a delete version.
-- A current business-state query excludes this deleted row.
SELECT refund_id, payment_id, amount, is_deleted, source_lsn, kafka_offset
FROM {silver} ORDER BY refund_id
;
SELECT refund_id, amount FROM {silver} WHERE NOT is_deleted
;
SELECT refund_id, COUNT(*) AS n FROM {silver}
GROUP BY refund_id HAVING COUNT(*) <> 1
