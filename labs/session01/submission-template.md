# 1회차 복습 과제 양식

이 파일은 수업 중 작성하지 않고 수업 후 복습에 사용합니다. 제출 방법과 기한은 수업 마무리의 강사 안내를 따릅니다.

이름 / 학생 ID:
작성일:

공통 orders·payments·refunds 예제의 예측과 수집 결과를 다시 살펴볼 수 있습니다. 개인 도메인·테이블·DDL 설계는 3회차에 진행합니다.

## 1. 왜 CDC 레이크하우스인가

- 전체 복사나 updated_at 조회보다 로그 기반 CDC가 필요한 상황 한 가지:
- CDC를 선택해도 관리해야 하는 비용·운영 문제 한 가지:

## 2. Iceberg와 아키텍처 역할 정리

| 구간·구성 요소 | 내 말로 한 줄 |
|---|---|
| PostgreSQL → Debezium → Kafka | |
| Flink → Iceberg Bronze | |
| MinIO / JDBC Catalog / Flink 복구 상태 | |
| Spark SQL Silver / dbt Gold — 후속 회차 | |

- Bronze·Silver·Gold와 Iceberg 내부 메타데이터 구조의 차이 한 문장:

## 3. 환경 확인과 ingest.sql 읽기

- student-worksheet.sql에서 확인한 내용이나 남은 질문:
- 강사가 확인한 preflight ready 상태: true / false / 미확인
- SQL [A]에 Catalog·raw 포맷·checkpoint·null 제외 조건의 역할을 정리했습니다: 예 / 작성 중
- Flink RUNNING이나 checkpoint 성공만으로 수집 완료라고 할 수 없는 이유:

학생은 맥미니 접속이나 로컬 Docker 실행을 하지 않습니다.

## 4. 공통 예제 사전 예측과 수집 결과 대조

강사 CASE_ID:
실제 환불 ID — CASE_ID-r1:
공통 토픽 / 파티션: cdc.public.refunds / 화면에서 확인
관찰 방식: 실제 시연 / 과거 리허설 기록 / 미관찰 (선택)

수업 후 자신의 예측과 실제 before/after·offset·copies를 아래 표에서 비교합니다. 확인하지 못한 값은 미확인으로 남기고 추정하지 않습니다. 전체 JSON을 옮길 필요는 없습니다.

| 변경(op) | 기대 원천 행 / before→after amount | 실제 before→after | Kafka offset | bronze_copies |
|---|---|---|---|---|
| INSERT(c) | | | | |
| UPDATE(u) | | | | |
| DELETE(d) | | | | |

### 다시 살펴볼 질문

- Kafka key의 refund_id와 원천 PK: 일치 / 불일치 / 미확인
- 원천의 환불 행: INSERT 뒤 1행·20000 / UPDATE 뒤 1행·30000 / DELETE 뒤 0행 확인 여부:
- Kafka와 Bronze의 같은 topic·partition·offset 및 내용: 일치 / 불일치 / 미확인
- DELETE 뒤 이번 환불 키의 Bronze history_ops:
- 이번 환불 키의 Kafka null tombstone 수:
- tombstone을 제외한 Bronze 이력 행 수:
- 원천은 0행인데 Bronze에는 c/u/d가 남는 이유:

### DELETE 요약의 지연 한 세트

| kafka_ts | ingested_at | wait_observed_seconds |
|---|---|---|
| | | |

이 값만으로 원천 commit부터 분석 마트 반영까지의 전체 지연을 계산할 수 없는 이유:

Kafka에 있으나 Bronze에 없을 때 먼저 확인할 구간 한 문장:

## 5. 복습 점검과 2회차 질문

- [ ] SQL [A] 네 가지 설정의 역할 정리
- [ ] SQL [B] UPDATE·DELETE 뒤 같은 PK를 조회하는 SELECT 작성
- [ ] SQL [C] 공통 환불 c/u/d 이벤트 예측
- [ ] 예측과 실제 수집 결과의 차이 확인 — 미확인 값은 추정하지 않기

다음 회차에서 확인하고 싶은 수집 설정·동작 질문 한 가지:

관심 도메인 메모 (선택, 설계 과제 아님):

2회차는 PostgreSQL → Debezium → Kafka 수집 심화입니다. 개인 프로젝트 설계 확정은 3회차에 진행하며, 오늘 개인 DDL·개인 DML은 작성하지 않습니다. 프로젝트 발표는 8회 스터디 뒤 별도 일정입니다.
