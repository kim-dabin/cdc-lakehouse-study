-- Common-example source-state check: aggregate before joining to avoid multiplying amounts.
WITH paid AS (
  SELECT order_id, sum(amount) AS amount FROM payments
  WHERE status = 'success' GROUP BY order_id
), refunded AS (
  SELECT p.order_id, sum(r.amount) AS amount
  FROM refunds r JOIN payments p ON r.payment_id = p.payment_id
  WHERE r.status = 'success' AND p.status = 'success'
  GROUP BY p.order_id
)
SELECT o.order_id,
       coalesce(p.amount, 0) AS paid,
       coalesce(r.amount, 0) AS refunded,
       coalesce(p.amount, 0) - coalesce(r.amount, 0) AS net_paid
FROM orders o
LEFT JOIN paid p ON o.order_id = p.order_id
LEFT JOIN refunded r ON o.order_id = r.order_id
WHERE o.order_id = :'namespace' || '-o1';
