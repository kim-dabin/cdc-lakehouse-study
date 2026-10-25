-- PostgreSQL / source-db / app. 읽기 전용이므로 각 단계 뒤 다시 실행합니다.
-- 첫 결과는 요약 3행입니다. 환불이 없어도 refunds의 current_rows=0인 행은 나옵니다.
SELECT 'orders' AS source_table, count(*) AS current_rows
FROM orders
WHERE order_id = :'case_id' || '-o1'
UNION ALL
SELECT 'payments', count(*)
FROM payments
WHERE payment_id = :'case_id' || '-p1'
UNION ALL
SELECT 'refunds', count(*)
FROM refunds
WHERE refund_id = :'case_id' || '-r1';

-- 두 번째 결과는 실제 현재 환불 행입니다. 단계에 따라 0행 또는 1행입니다.
SELECT refund_id, payment_id, amount, status
FROM refunds
WHERE refund_id = :'case_id' || '-r1';
