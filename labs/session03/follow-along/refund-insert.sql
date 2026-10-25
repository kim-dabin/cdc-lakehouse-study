-- PostgreSQL / source-db / app
-- 준비한 결제에 환불 1건을 만듭니다. 같은 CASE_ID로 두 번 실행하지 않습니다.
BEGIN;

INSERT INTO refunds (refund_id, payment_id, amount, status)
VALUES (:'case_id' || '-r1', :'case_id' || '-p1', 20000, 'success');

COMMIT;
