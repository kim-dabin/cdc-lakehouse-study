-- 1회차 학생 작성 파일. 이 파일은 제출물이며 자동 실행되지 않습니다.
-- 강사가 승인한 실습 SQL만 맥미니에 반영합니다.

-- [A] 예제의 키와 금액을 바꾸고 예상 순결제액을 적으세요.
-- 내 이름/학생 ID:
-- 변경 조건:
-- 결제 직후 기대값:
-- 환불 직후 기대값:
-- 환불 정정 후 기대값:

BEGIN;
INSERT INTO orders (order_id, ordered_at, status)
VALUES ('my-order-01', '2026-10-19T10:00:00+09:00', 'confirmed');
INSERT INTO payments (payment_id, order_id, amount, status)
VALUES ('my-payment-01', 'my-order-01', 100000, 'success');
COMMIT;

INSERT INTO refunds (refund_id, payment_id, amount, status)
VALUES ('my-refund-01', 'my-payment-01', 20000, 'success');

UPDATE refunds SET amount = 30000 WHERE refund_id = 'my-refund-01';

-- [B] 원천 데이터만으로 내 주문의 순결제액을 계산하세요.
-- 결제와 환불을 각각 집계한 뒤 연결합니다. 결제가 여러 건일 수 있습니다.
-- TODO: SELECT ...

-- [C] 내 프로젝트에서 수정이 발생하는 테이블 하나를 설계하세요.
-- 한 행의 의미:
-- PK:
-- 수정 가능한 컬럼:
-- 관련 테이블과의 관계:
-- TODO: CREATE TABLE ... (PRIMARY KEY 포함, FK 또는 관계 설명 포함)

-- [D] 입력, 수정, 삭제의 DML을 각각 한 개 작성하고 지표의 기대값을 적으세요.
-- TODO: INSERT ...
-- TODO: UPDATE ... WHERE ...
-- TODO: DELETE ... WHERE ...
