-- 영향 키가 사라졌어도 source에 남겨 Gold에서 지워요.
MERGE INTO {gold_order} t
USING (
  SELECT k.order_id, n.order_date, n.paid, n.refunded, n.net_paid,
         n.order_id IS NOT NULL present
  FROM {affected_keys} k LEFT JOIN {full_order} n ON k.order_id=n.order_id
) s ON t.order_id=s.order_id
WHEN MATCHED AND NOT s.present THEN DELETE
WHEN MATCHED THEN UPDATE SET
  t.order_date=s.order_date, t.paid=s.paid,
  t.refunded=s.refunded, t.net_paid=s.net_paid
WHEN NOT MATCHED AND s.present THEN INSERT (order_id,order_date,paid,refunded,net_paid)
VALUES (s.order_id,s.order_date,s.paid,s.refunded,s.net_paid)
