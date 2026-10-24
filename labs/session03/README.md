# 3회차 — Flink → Bronze 적재 심화, 개인 도메인·테이블 설계 확정

**공통 주문·결제·환불 예제 · Flink 적재와 지연 · Iceberg·MinIO·운영 · 개인 설계 확정**

1회차에 전체 구조를 보고 2회차에 PostgreSQL → Debezium → Kafka 수집을 확인한 `orders`·`payments`·`refunds` 공통 예제를 이어갑니다. 결제 100,000원에 환불 20,000원 입력 → 30,000원 정정 → 환불 행 삭제를 따라 Flink → Bronze 적재와 지연을 확인합니다. 이 예제의 삭제는 환불 철회를 뜻합니다.

Flink와 Iceberg가 무엇인지부터 배웁니다. 이미 배운 Kafka·Spark·S3와 역할을 나누고, 배치·Spark Structured Streaming·Connect sink와 비교해 왜 이 구성을 선택했는지 생각합니다. 실제 raw 적재 SQL을 읽은 뒤 writer가 파일을 준비하고 checkpoint 상태를 거쳐 테이블을 commit하는 과정을 공통 예제에 연결합니다.

- [수업 슬라이드 PPTX](slides/03-session03.pptx) · [PDF](slides/03-session03.pdf): 공통 예제와 개념·선택·관찰·설계 흐름.
- [학생 가이드](student-guide.md): 첫날 로드맵과 이번 수업의 순서, 개념 설명·관찰·운영 판단 질문.
- [설계 학습지](design-worksheet.md): 공통 주문·결제·환불 작성 예시와 개인 도메인·테이블 확정.
- [파일·metadata 관찰 SQL](metadata-inspection.sql): Spark 조회 네 가지와 별도 Catalog PostgreSQL 조회. 연결을 구분해 한 SELECT씩 사용합니다.
- [새 환불의 지연 관찰](student-guide.md#관찰-c--kafka에-있으면-bronze에도-보이나요-지연은-어디서부터-재나요): 주문·결제까지 준비한 새 키에서 INSERT 호출·commit 응답·첫 Bronze SELECT의 경계를 구분합니다.
- [공식 참고 문서](references.md): 선택·내부 동작·운영 판단의 근거.
- [개인 공통 예제 실행](../../docs/self-study.md): 저장소 루트의 기존 실행 스크립트와 개인 Docker 환경 결과 확인.
- [전체 아키텍처](../../docs/architecture.md): 데이터 흐름·저장·실행 환경.

수업에서는 강사 맥미니 공유 화면을 보며 학생도 실행 전 예상, SQL 작성·읽기, 실제 수집 결과 대조에 참여합니다. 집에서는 제공된 공통 전체 실행 프로그램을 개인 Docker 환경에서 실행합니다. MinIO에서 `warehouse/bronze/cdc_events`의 `data`·`metadata`를 보는 관찰도 수업에 포함됩니다. 현재 수업 콘솔에서는 JSON Preview가 되지 않으므로, 강사가 사전에 Download한 JSON을 로컬 편집기에서 열고 당시 Catalog pointer·객체 경로·Download 시각을 대조합니다. 콘솔의 경로·크기·시각과 snapshots·files·metadata 이력·같은 키 SELECT를 연결합니다.

전체 흐름은 첫날 로드맵과 공통 원천 구조 → 문제와 요구 → 도구 정의·대안 → Flink 입력·출력 → Iceberg·commit → 공통 이력·MinIO·지연 관찰 → 운영 판단입니다. 공통 c/u/d·checkpoint·commit을 확인한 뒤 **개인 설계 30분과 리뷰 12분**을 진행합니다. 각자 선택한 도메인에서 원천 2~3개 테이블의 한 행·PK/FK·시간과 순서 기준·이벤트 유형·삭제 정책, 토픽·Bronze 보존 정보와 후속 조인·집계 키를 확정합니다. 개인 프로젝트를 주문 예제로 통일하지 않으며 실제 DDL 구현은 필수가 아닙니다.

Flink checkpoint는 별도 볼륨의 복구 상태이고 Iceberg commit은 테이블 변경 경계입니다. 파일이 생겼다는 사실이나 checkpoint 성공만으로 이번 키의 조회를 단정하지 않습니다. 현재 raw Bronze는 append이며 업무 키 upsert·업무 중복 제거를 하지 않습니다. 단일 원천·토픽별 1파티션·parallelism 1인 학습 환경의 한계도 함께 다룹니다.

기존 CASE의 이력·MinIO·SQL 대조를 끝낸 뒤, 강사는 Linux runner의 측정 도구로 새 환불 20,000원 INSERT와 첫 Bronze 조회를 한 번에 관찰합니다. 별도로 환불을 입력하지 않습니다. commit 응답 시각은 DB 내부 commit 시각이 아니며 polling·조회 시간이 포함됩니다. 기존 wait 관찰값이나 Iceberg commit 소요·SLA와 구분하고, timeout·commit 결과 불명확 시 재시도나 정리로 덮지 않습니다.

4회차에는 내 프로젝트의 Spark Silver 중복 제거·MERGE, 5회차에는 dbt Gold·테스트, 6회차에는 수정·삭제·소급 정정을 구현합니다. 7회차는 Iceberg rewrite_data_files·파티션·쿼리 튜닝, 8회차는 Airflow 연동·장애 복구·최종 리뷰로 이어집니다. 제출 방법과 기한은 공지를 따릅니다. **최종 발표는 8회 스터디 이후 별도 일정입니다.**
