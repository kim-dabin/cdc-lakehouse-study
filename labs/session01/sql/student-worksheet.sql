-- 1회차 공통 예제의 코드 읽기·SQL 작성·이벤트 예측용 파일입니다.
-- 강사는 맥미니에서 실행하고, 학생은 로컬 파일만 작성합니다.
-- 전체 파일을 실행하지 마세요. [B]는 PostgreSQL, [D]는 Spark SQL입니다.
-- my-r1은 예측용 이름입니다. 강사의 실제 CASE_ID-r1과 구분합니다.
-- 이름 / 학생 ID:

-- [A] flink/ingest.sql.template 읽기 — 답은 주석으로 적습니다.
-- 1. JdbcCatalog와 warehouse는 각각 무엇을 가리키나요?
-- 답:
-- 2. topic-pattern과 format='raw'는 무엇을 읽어 payload에 저장하나요?
-- 답:
-- 3. checkpoint 완료 외에 외부 조회까지 무엇을 확인해야 하나요?
-- 답:
-- 4. WHERE payload IS NOT NULL은 무엇을 제외하고 어떤 이벤트를 남기나요?
-- 답:

-- [B] PostgreSQL 공통 예제 — 같은 환불 한 건을 입력·정정·삭제합니다.
-- orders·payments는 준비 데이터이며 아래 두 SELECT만 추가 작성합니다.
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
-- TODO: 같은 refund_id의 현재 값이 1행·30000인지 확인하는 SELECT

DELETE FROM refunds WHERE refund_id = 'my-r1';
-- TODO: 같은 refund_id가 0행인지 확인하는 SELECT

-- [C] Kafka CDC 이벤트의 사전 예측
-- 예제 원천 테이블에는 REPLICA IDENTITY FULL이 설정되어 있습니다.
-- INSERT: key / op / before.amount / after.amount:
-- UPDATE: key / op / before.amount / after.amount:
-- DELETE: key / op / before.amount / after.amount:
-- DELETE의 after가 null일 때 원천 PK를 찾을 곳:
-- Kafka와 Bronze의 같은 수집 위치를 대조하는 세 항목:

-- [D] Spark SQL 참고 — 이번 환불 키의 Bronze 이력만 조회합니다.
-- 읽기용 예제이며 학생이 서버에 접속해 실행하는 과제가 아닙니다.
-- 실제 조회 시 my-r1을 강사의 CASE_ID-r1로 바꿉니다.
SELECT get_json_object(payload, '$.op') AS op,
       get_json_object(payload, '$.before.amount') AS before_amount,
       get_json_object(payload, '$.after.amount') AS after_amount,
       topic, partition_id, kafka_offset
FROM lake.bronze.cdc_events
WHERE topic = 'cdc.public.refunds'
  AND (get_json_object(payload, '$.after.refund_id') = 'my-r1'
       OR get_json_object(payload, '$.before.refund_id') = 'my-r1')
ORDER BY partition_id, kafka_offset;

-- 실제 before/after·offset·copies는 submission.md 4절에 기록합니다.
-- 개인 테이블 DDL·DML 설계는 이번 제출·과제에 포함하지 않습니다.
