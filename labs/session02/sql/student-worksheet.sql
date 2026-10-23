-- 2회차 — 원천 설정부터 Kafka·Bronze·파일까지 같은 변경 따라가기
-- 수업 중에는 강사 화면을 보며 SQL·예측을 작성합니다.
-- 수업 후에는 개인 환경에서 공통 예제를 실행하고 실제 결과와 대조합니다.
-- 이 파일을 통째로 실행하지 마세요. [A][D]는 Spark SQL, [B]는 PostgreSQL입니다.
-- [C][E]는 읽기·예측 질문입니다. Python 파서의 실행 방법은 학생 가이드를 봅니다.
-- YOUR_REFUND_ID는 강사 시연 또는 개인 실행에서 확인한 실제 환불 ID로 바꿉니다.
-- 이름 / 학생 ID:
-- 개인 실행 CASE_ID / 환불 ID:

-- [A] Spark SQL — Bronze의 c/u/d 세 이력과 파일
-- 1. 같은 환불을 20000 입력 → 30000 정정 → 삭제하면 어떻게 될까요?
-- 예상 원천 현재 행 수:
-- 예상 Bronze 이력 수와 op 순서:
-- DELETE 이벤트에서 refund_id를 찾을 곳:

-- 아래 쿼리는 after.refund_id만 조건으로 사용합니다.
-- TODO: DELETE 이력도 포함하도록 before.refund_id 조건을 추가하세요.
SELECT get_json_object(payload, '$.op') AS op,
       get_json_object(payload, '$.before.amount') AS before_amount,
       get_json_object(payload, '$.after.amount') AS after_amount,
       get_json_object(payload, '$.source.lsn') AS source_lsn,
       topic, partition_id, kafka_offset, _file AS parquet_file
FROM lake.bronze.cdc_events
WHERE topic = 'cdc.public.refunds'
  AND get_json_object(payload, '$.after.refund_id') = 'YOUR_REFUND_ID'
ORDER BY partition_id, kafka_offset;

-- 2. Iceberg 현재 테이블이 참조하는 데이터 파일 목록입니다.
-- 전체 테이블의 파일 목록이며 위 환불 ID만의 파일 목록은 아닙니다.
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM lake.bronze.cdc_events.files
WHERE content = 0
ORDER BY file_path
LIMIT 10;

-- 위 결과는 데이터 파일 일부입니다. [A]-1의 parquet_file과 file_path를 연결합니다.
-- 해당 파일이 첫 10개에 없다면 file_path 조건으로 직접 찾습니다.

-- TODO: 강사가 실제 Parquet 내용을 읽는 화면에서 아래를 찾아보세요.
-- 파일에 저장된 payload·topic·partition_id·kafka_offset:
-- Kafka value 원문에 있던 op와 금액은 파일의 어느 컬럼에 들어 있나요?
-- 세 이벤트가 반드시 한 파일에 들어가거나 DML 하나마다 파일 하나가 생길까요?
-- MinIO 폴더에 있는 모든 파일과 Iceberg 현재 테이블이 참조하는 파일은 같을까요?

-- [B] PostgreSQL — 수집 조건을 읽기 전용으로 확인하기
-- 아래 SELECT/SHOW는 원천 PostgreSQL에만 실행합니다.
-- publication·slot을 삭제하거나 replica identity를 변경하는 실습이 아닙니다.
SHOW wal_level;

SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete
FROM pg_publication
WHERE pubname = 'cdc_lab';

SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'cdc_lab'
ORDER BY schemaname, tablename;

SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       c.relreplident AS replica_identity
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN ('orders', 'payments', 'refunds')
ORDER BY c.relname;

SELECT slot_name, plugin, slot_type, database, active,
       restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'cdc_lab';

-- TODO: 아래 역할을 각각 한 줄로 설명하세요.
-- publication — 어떤 테이블/변경을 발행하는가:
-- replication slot — 어느 소비자의 진행 위치·WAL 보존과 관련되는가:
-- REPLICA IDENTITY FULL — before.amount 관찰과 어떤 관계가 있는가:
-- slot active=true이면 Bronze 반영까지 완료됐다고 할 수 있나요?

-- [C] Debezium·Kafka — 설정과 메시지를 연결하기
-- ./labs/session02/lab.sh connector-sanitized 출력에서 비밀번호를 제외한 설정을 읽습니다.
-- snapshot.mode='initial'과 새 DML의 op=c/u/d는 어떻게 다른가요?
-- 이미 수집 중인 커넥터를 재기동하면 매번 처음부터 r을 발행하나요?
-- table.include.list와 PostgreSQL publication 중 한쪽에만 테이블이 있으면 충분할까요?
-- topic.prefix='cdc'일 때 refunds 토픽 이름:
-- 같은 환불의 Kafka key와 value 안의 refund_id는 각각 어디에 있나요?
-- op=d의 value와 tombstone의 value는 어떻게 다른가요?
-- 같은 이벤트를 Kafka와 Bronze에서 대조할 세 항목:
-- PK / source.lsn / Kafka 위치 중 단독 전역 고유 이벤트 ID로 쓰면 안 되는 것과 이유:

-- [D] Spark SQL — 작은 파서의 해석을 SQL로 다시 표현하기
-- Python 파서는 원문 한 건을 해석합니다. 아래도 같은 기준으로 JSON을 읽습니다.
-- 현재 상태를 갱신하거나 Silver에 MERGE하는 쿼리가 아닙니다.
WITH decoded AS (
  SELECT get_json_object(payload, '$.op') AS op,
         get_json_object(payload, '$.before.refund_id') AS before_id,
         get_json_object(payload, '$.after.refund_id') AS after_id,
         CAST(get_json_object(payload, '$.before.amount') AS BIGINT) AS before_amount,
         CAST(get_json_object(payload, '$.after.amount') AS BIGINT) AS after_amount,
         topic, partition_id, kafka_offset
  FROM lake.bronze.cdc_events
  WHERE topic = 'cdc.public.refunds'
)
SELECT op,
       COALESCE(after_id, before_id) AS refund_id,
       CASE WHEN op = 'd' THEN before_amount ELSE after_amount END AS event_amount,
       topic, partition_id, kafka_offset
FROM decoded
WHERE COALESCE(after_id, before_id) = 'YOUR_REFUND_ID'
ORDER BY partition_id, kafka_offset;

-- TODO: Python 파서와 위 SQL을 읽고 답하세요.
-- 실제 20000→30000→DELETE 시나리오에서 c / u / d의 event_amount 기대값:
-- DELETE에서 after만 읽으면 무엇을 잃나요?
-- r 이벤트를 새 INSERT c와 구분해야 하는 이유:
-- tombstone을 일반 JSON 객체처럼 읽으면 어떻게 될까요?
-- 손상된 JSON이나 모르는 op를 만났을 때 무엇을 알려줘야 할까요?
-- 고정 fixture는 각각 독립 입력입니다. 실제 시나리오의 금액/순서와 구분합니다.
-- 파서 fixture를 통과하면 실제 DB→Bronze 수집도 검증됐다고 할 수 있나요?

-- [E] 진단 — 가장 먼저 볼 구간과 필요한 증거를 적습니다.
-- 상황 1: 원천 UPDATE는 보이는데 Kafka에 해당 키의 u가 없습니다.
-- 먼저 볼 구간 / 필요한 증거:
-- 상황 2: Kafka에는 u가 있지만 Bronze에는 없습니다.
-- 먼저 볼 구간 / 필요한 증거:
-- 상황 3: Bronze에서 c와 u만 조회됩니다. DELETE가 누락됐을까요?
-- 먼저 볼 조회 조건 / 비교할 원천·Kafka 증거:
-- 상황 4: Parquet 한 파일에는 c만 있습니다. u/d 수집 실패일까요?
-- 먼저 볼 파일 목록·테이블 조회 / 판단을 보류할 이유:

-- 수업 중 submission.md를 새로 작성할 필요는 없습니다.
-- 수업 후 개인 실행의 결과·캡처·질문은 복습 양식에 정리합니다.
-- 강사 시연·고정 fixture·과거 리허설 값을 개인 실행 결과처럼 적지 않습니다.
-- 개인 도메인·테이블 설계 확정은 3회차입니다.
