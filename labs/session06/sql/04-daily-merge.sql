-- 영향 날짜만 다시 묶어요. 주문이 0건인 날짜는 지워요.
MERGE INTO {gold_daily} t
USING (
  SELECT d.order_date, count(o.order_id) order_count,
         sum(o.paid) paid, sum(o.refunded) refunded, sum(o.net_paid) net_paid
  FROM {affected_dates} d LEFT JOIN {gold_order} o ON d.order_date <=> o.order_date
  GROUP BY d.order_date
) s ON t.order_date <=> s.order_date
WHEN MATCHED AND s.order_count=0 THEN DELETE
WHEN MATCHED THEN UPDATE SET
  t.order_count=s.order_count, t.paid=s.paid,
  t.refunded=s.refunded, t.net_paid=s.net_paid
WHEN NOT MATCHED AND s.order_count>0 THEN INSERT (order_date,order_count,paid,refunded,net_paid)
VALUES (s.order_date,s.order_count,s.paid,s.refunded,s.net_paid)
