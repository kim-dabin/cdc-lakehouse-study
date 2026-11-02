-- 결제와 환불을 먼저 묶어서 조인으로 금액이 늘어나지 않게 해요.
CREATE OR REPLACE TEMP VIEW {full_order} AS
WITH payments AS (
  SELECT order_id, sum(amount) paid FROM {fct_payment}
  WHERE status='success' GROUP BY order_id
), refunds AS (
  SELECT order_id, sum(amount) refunded FROM {fct_refund}
  WHERE status='success' AND payment_status='success' GROUP BY order_id
)
SELECT o.order_id, o.order_date,
       coalesce(p.paid,0) paid, coalesce(r.refunded,0) refunded,
       coalesce(p.paid,0)-coalesce(r.refunded,0) net_paid
FROM {dim_order} o
LEFT JOIN payments p ON o.order_id=p.order_id
LEFT JOIN refunds r ON o.order_id=r.order_id
;
CREATE OR REPLACE TEMP VIEW {full_daily} AS
SELECT order_date, count(*) order_count, sum(paid) paid,
       sum(refunded) refunded, sum(net_paid) net_paid
FROM {full_order} GROUP BY order_date
