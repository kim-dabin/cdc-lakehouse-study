# 1회차 제출 — 이름 / 학생 ID

## 1. 아키텍처를 보며 역할 정리

| 구간 | 내 말로 한 줄 |
|---|---|
| PostgreSQL → Debezium | |
| Kafka → Flink → Bronze | |
| 객체 저장소 / Catalog / 복구 상태 | |

## 2. 시연 전 예측과 시연 중 관찰

**시연 전** 기대 원천 행과 before/after를 채웁니다. **시연 중**에는 오른쪽 3칸만 기록합니다.

강사 CASE_ID:
공통 토픽 / 파티션: cdc.public.refunds / 화면에서 확인
관찰 방식: 실제 시연 / 과거 리허설 기록 (선택)

| 변경(op) | 기대 원천 행 / before→after amount | 실제 before→after | Kafka offset | bronze_copies |
|---|---|---|---|---|
| INSERT(c) | | | | |
| UPDATE(u) | | | | |
| DELETE(d) | | | | |

- 강사가 확인한 Kafka key와 원천 PK의 일치 여부:
- DELETE 후 원천 0행 / Bronze c,u,d 이력 보관 확인:
- Kafka null tombstone과 Bronze 행 수 차이:

### 시연 뒤 지연 관찰 — DELETE 한 세트만

| kafka_ts | ingested_at | wait_observed_seconds |
|---|---|---|
| | | |

이 값만으로 원천 commit부터 조회 가능 시점까지의 전체 지연을 계산할 수 없는 이유:

Kafka에 있으나 Bronze에 없을 때 먼저 확인할 구간 (한 문장):

## 3. 개인 설계 — 수업 중 필수

- 주제 / 데이터 소비 목적:
- CDC를 선택하는 이유:
- 나중에 추가할 테이블 이름 1–2개 (이름만):

| 핵심 테이블 하나 | 한 행의 의미 | PK / 관계 | 수정 컬럼 / 삭제 방식 |
|---|---|---|---|
| | | | |

파이프라인 한 줄: PostgreSQL 내 테이블 → Debezium → Kafka 내 토픽 → Flink → Iceberg Bronze

| 내 변경 | 대상 PK | 기대 op와 before/after |
|---|---|---|
| INSERT | | |
| UPDATE | | |
| DELETE | | |

DDL 하나를 `student-worksheet.sql` [C]에 작성했습니다: 예 / 작성 중

이번 실습에서는 이전 값을 관찰하기 위해 PK와 REPLICA IDENTITY FULL을 포함합니다. 수집 권한·publication·connector·Flink 연결은 다음 회차에 확인합니다.

## 4. 다음 회차 전 과제

- [ ] 나머지 테이블 1–2개 명세·DDL과 관계 보완
- [ ] 개인 테이블의 c/u/d DML과 원천 확인 SELECT 작성
- [ ] 수집 범위·목표 지연·이력 보존과 연결 설정 초안 보완
- 미정 항목 / 질문:

오늘은 위 1–3절과 SQL [A]의 원천 SELECT, [C]의 DDL 초안을 제출합니다. 개인 DML·SELECT 전체는 과제로 완성합니다. 프로젝트 발표는 8회 스터디 뒤 별도 일정입니다.
