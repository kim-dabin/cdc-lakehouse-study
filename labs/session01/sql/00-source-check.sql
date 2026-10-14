SELECT order_id, ordered_at, status FROM orders
WHERE order_id = :'namespace' || '-o1';
SELECT payment_id, order_id, amount, status FROM payments
WHERE payment_id = :'namespace' || '-p1';
SELECT refund_id, payment_id, amount, status FROM refunds
WHERE refund_id = :'namespace' || '-r1';
