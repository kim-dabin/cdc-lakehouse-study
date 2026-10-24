# 3회차 — Flink → Bronze 적재 심화, 개인 도메인·테이블 설계 확정

**본편 약 215분(휴식 10분 + 5분 포함) · 정의와 선택 이유 · 내부 동작·운영 · MinIO 연결 관찰 · 개인 설계**

Flink와 Iceberg가 무엇인지부터 배웁니다. 이미 배운 Kafka·Spark·S3와 역할을 나누고, 배치·Spark Structured Streaming·Connect sink와 비교해 왜 이 구성을 선택했는지 생각합니다. 실제 raw 적재 SQL을 읽은 뒤 writer가 파일을 준비하고 checkpoint 상태를 거쳐 테이블을 commit하는 과정을 따라갑니다.

- [수업 슬라이드 PPTX](slides/03-session03.pptx) · [PDF](slides/03-session03.pdf): 본편과 선택 부록.
- [학생 가이드](student-guide.md): 약 215분 흐름, 개념 설명·관찰·운영 판단 질문.
- [설계 학습지](design-worksheet.md): 공간 예약 예시와 개인 도메인·테이블 확정.
- [파일·metadata 관찰 SQL](metadata-inspection.sql): Spark 조회 네 가지와 별도 Catalog PostgreSQL 조회. 연결을 구분해 한 SELECT씩 사용합니다.
- [공식 참고 문서](references.md): 선택·내부 동작·운영 판단의 근거.
- [개인 공통 예제 실행](../../docs/self-study.md): 기존 개인 Docker 환경의 실행 계획·결과 확인.
- [전체 아키텍처](../../docs/architecture.md): 데이터 흐름·저장·실행 환경.

수업 중 실행은 강사 맥미니 화면에서 봅니다. 학생은 예상·SQL 읽기·설계 판단에 참여하며 새 환경 설치나 긴 명령 입력을 수업 진도로 삼지 않습니다. MinIO에서 `warehouse/bronze/cdc_events`의 `data`·`metadata`를 보는 **10분 관찰은 본편에 포함**됩니다. 콘솔에서 경로·크기·시각을 보고 JSON 내용은 강사가 사전 준비한 로컬 편집기로 확인합니다. Catalog의 현재 참조와 snapshots·files·metadata 이력·같은 키 SELECT를 연결합니다.

전체 흐름은 문제와 요구 → 도구 정의 → 대안 → Flink 입력·출력 → Iceberg·commit → 연결 관찰 → 운영 판단입니다. 개인 설계 전 짧은 휴식을 두고 설계 30분과 리뷰 12분을 남깁니다. 리뷰는 대표 두 사례 8분과 전원 자가 점검 4분으로 진행합니다. 원천 2~3개 테이블의 한 행·PK/FK·시간과 순서 기준·이벤트 유형·삭제 정책, 토픽·Bronze 보존 정보와 후속 조인·집계 키를 확정하며 실제 DDL 구현은 필수가 아닙니다.

Flink checkpoint는 별도 볼륨의 복구 상태이고 Iceberg commit은 테이블 변경 경계입니다. 파일이 생겼다는 사실이나 checkpoint 성공만으로 이번 키의 조회를 단정하지 않습니다. 현재 raw Bronze는 append이며 업무 키 upsert·업무 중복 제거를 하지 않습니다. 단일 원천·토픽별 1파티션·parallelism 1인 학습 환경의 한계도 함께 다룹니다.

Silver·Gold 구현은 후속 회차로 이어갑니다. 선택 부록 전체를 새 과제로 요구하지 않으며 제출 방법과 기한은 공지를 따릅니다. **최종 발표는 8회 스터디 이후 별도 일정입니다.**
