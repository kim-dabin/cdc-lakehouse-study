-- PostgreSQL / source-db / app
-- 같은 환불의 금액을 정정합니다. UPDATE 반복은 CDC 이력을 더 만들 수 있습니다.
BEGIN;

UPDATE refunds
SET amount = 30000
WHERE refund_id = :'case_id' || '-r1';

COMMIT;
