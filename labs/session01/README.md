# 1회차 — CDC 레이크하우스 이해와 공통 예제 수집 확인

**120분 · 온라인 · 강사 맥미니 시연 + 학생 SQL 작성·수집 결과 대조**

공통 orders·payments·refunds 예제로 전체 구조를 익힌다. 같은 환불 한 건의 INSERT·UPDATE·DELETE를 먼저 예측하고 PostgreSQL·Kafka·Iceberg Bronze에서 실제 결과를 대조한다. 개인 도메인·테이블 설계 확정은 3회차에 진행한다.

## 수업에 사용할 자료

- [학생 실습 가이드와 120분 흐름](student-guide.md)
- [학생 SQL — 코드 읽기·공통 예제 예측](sql/student-worksheet.sql)
- [수업 후 복습 양식 — 수업 중 사용 안 함](submission-template.md)
- [개인 환경 기동·접속·운영 안내](../../docs/operations.md)
- [데이터 흐름·저장/메타데이터·실행 환경 세 장](architecture.md)
- [왜 CDC를 배우는가 / 후속 프로젝트 예시](why-cdc-and-projects.md)

OT → 도입 배경 → 개념·아키텍처 → 환경·예측 → 공통 수집 대조 순서로 진행한다. 수업과 복습에서 할 일은 학생 실습 가이드를 따른다.

![데이터 흐름](../../docs/diagrams/01-data-flow.png)

## 수업 후 남는 것

1. CDC 선택 이유와 관리해야 하는 비용, 수집·저장·Catalog·복구 상태의 역할 이해.
2. 같은 환불의 20000 입력 → 30000 정정 → 삭제에 대한 사전 예측 SQL과 기대 이벤트.
3. 원천 현재 행·Kafka·Bronze의 before/after·수집 위치·copies를 대조한 경험.
4. 삭제 후 남는 이력, tombstone 처리와 지연 값의 의미, 다음 회차 질문.

수업 중에는 학생이 로컬 Docker나 SSH 없이 student-worksheet.sql 하나로 코드 읽기·SELECT 작성·이벤트 예측을 진행한다. 수업 중 submission.md 작성이나 파일 제출은 하지 않으며 수집 결과 메모는 선택이다. 수업 후에는 개인 환경에서 공통 예제를 실행하고 복습 결과를 정리한다. 오늘 개인 테이블 DDL·DML은 작성하지 않는다. 수집 시연에서는 Silver·Gold refresh와 Airflow를 실행하지 않는다. 프로젝트 발표는 8회 스터디 뒤 별도 일정이다.
