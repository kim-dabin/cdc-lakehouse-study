-- 1회차: 학생은 로컬에서 작성합니다. 강사가 검토한 SQL만 맥미니에서 실행합니다.
-- 이름 / 학생 ID:

-- [A] FK를 맞추고 같은 환불 PK에 입력·수정·삭제를 작성합니다.
-- 단계마다 원천 SELECT와 Kafka·Bronze의 기대 이벤트를 submission.md에 적으세요.
BEGIN;
INSERT INTO orders (order_id, ordered_at, status)
VALUES ('my-o1', '2026-10-19T10:00:00+09:00', 'confirmed');
INSERT INTO payments (payment_id, order_id, amount, status)
VALUES ('my-p1', 'my-o1', 100000, 'success');
COMMIT;

INSERT INTO refunds (refund_id, payment_id, amount, status)
VALUES ('my-r1', 'my-p1', 20000, 'success');
SELECT refund_id, payment_id, amount, status
FROM refunds WHERE refund_id = 'my-r1';

UPDATE refunds SET amount = 30000 WHERE refund_id = 'my-r1';
-- TODO: 동일 PK의 현재 값을 확인하는 SELECT

DELETE FROM refunds WHERE refund_id = 'my-r1';
-- TODO: 삭제 후 해당 PK가 0행임을 확인하는 SELECT

-- [B] Kafka CDC 이벤트의 기대값을 기록합니다.
-- INSERT: key / op / before / after:
-- UPDATE: key / op / before / after:
-- DELETE: key / op / before / after:
-- Kafka와 Bronze를 연결하는 수집 위치:

-- [C] 개인 프로젝트의 원천 테이블 하나를 설계합니다.
-- 테이블명 / 한 행의 의미:
-- PK / FK 또는 관련 테이블:
-- 수정 가능한 컬럼 / 물리 삭제 또는 논리 삭제:
-- TODO: CREATE TABLE ... (PRIMARY KEY 포함)

-- [D] 내 테이블의 변경 테스트와 원천 확인 SQL
-- TODO: INSERT ...
-- TODO: SELECT ... WHERE PK = ...
-- TODO: UPDATE ... WHERE PK = ...
-- TODO: SELECT ... WHERE PK = ...
-- TODO: DELETE ... WHERE PK = ...
-- TODO: SELECT ... WHERE PK = ...
-- 각 DML의 기대 key·op·before/after와 Bronze 검증 계획은 submission.md에 적습니다.
