-- PostgreSQL / source-db / app
-- 이 예제에서 환불 행의 물리 삭제는 환불 철회를 뜻합니다.
-- CDC에는 op=d가 남고, 기존 Bronze의 c/u 이력은 삭제되지 않습니다.
BEGIN;

DELETE FROM refunds
WHERE refund_id = :'case_id' || '-r1';

COMMIT;
