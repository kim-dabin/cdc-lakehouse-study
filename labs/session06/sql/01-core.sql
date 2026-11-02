-- 현재 상태에서 삭제 표시를 제외하고 Silver core와 같은 의미로 계산해요.
CREATE OR REPLACE TEMP VIEW {dim_order} AS
SELECT order_id, ordered_at,
       to_date(from_utc_timestamp(ordered_at, 'Asia/Seoul')) order_date, status
FROM {input_orders} WHERE NOT is_deleted
;
CREATE OR REPLACE TEMP VIEW {fct_payment} AS
SELECT payment_id, order_id, amount, status
FROM {input_payments} WHERE NOT is_deleted
;
CREATE OR REPLACE TEMP VIEW {fct_refund} AS
SELECT r.refund_id, r.payment_id, p.order_id, r.amount, r.status,
       p.status payment_status
FROM {input_refunds} r LEFT JOIN {fct_payment} p ON r.payment_id=p.payment_id
WHERE NOT r.is_deleted
