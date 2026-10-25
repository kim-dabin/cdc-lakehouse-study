-- PostgreSQL / source-db / app
-- psql -v case_id="$CASE_ID"로 받은 값을 SQL 문자열로 안전하게 사용합니다.
-- 새 CASE_ID로 한 번만 실행합니다. 주문 1건과 결제 1건을 함께 만듭니다.
BEGIN;

INSERT INTO orders (order_id, ordered_at, status)
VALUES (:'case_id' || '-o1', '2026-10-19T10:00:00+09:00', 'confirmed');

INSERT INTO payments (payment_id, order_id, amount, status)
VALUES (:'case_id' || '-p1', :'case_id' || '-o1', 100000, 'success');

COMMIT;
