INSERT INTO refunds (refund_id, payment_id, amount, status)
VALUES (:'namespace' || '-r1', :'namespace' || '-p1', 20000, 'success');
