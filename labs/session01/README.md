# 1회차 — CDC 레이크하우스 이해와 공통 예제 수집 확인

**120분 · 온라인 · 강사 맥미니 시연 + 학생 SQL 작성·수집 결과 대조**

공통 orders·payments·refunds 예제로 전체 구조를 익힌다. 같은 환불 한 건의 INSERT·UPDATE·DELETE를 먼저 예측하고 PostgreSQL·Kafka·Iceberg Bronze에서 실제 결과를 대조한다. 개인 도메인·테이블 설계 확정은 3회차에 진행한다.

## 수업에 사용할 자료

- [학생 실습 가이드와 120분 흐름](student-guide.md)
- [학생 SQL — 코드 읽기·공통 예제 예측](sql/student-worksheet.sql)
- [수업 후 복습 양식 — 수업 중 사용 안 함](submission-template.md)
- [강사용 120분 진행안](instructor-guide.md)
- [맥미니 시연 명령·포트·장애 대응](runbook.md)
- [데이터 흐름·저장/메타데이터·실행 환경 세 장](architecture.md)
- [왜 CDC를 배우는가 / 후속 프로젝트 예시](why-cdc-and-projects.md)
- [2026-10-18 수정 큐시트 순서 재실행·재기동 검증](cuesheet-rerun-20261018.md)

진행 기준은 강사가 사용하는 최종 강의안의 OT → 도입 배경 → 개념·아키텍처 → 환경·예측 → 공통 수집 대조 순서다. 저장소의 [이전 PPT](slides/README.md)는 과거 초안으로 보관하며 현재 수업의 활동 기준으로 사용하지 않는다.

![데이터 흐름](../../docs/diagrams/01-data-flow.png)

## 수업 후 남는 것

1. CDC 선택 이유와 관리해야 하는 비용, 수집·저장·Catalog·복구 상태의 역할 이해.
2. 같은 환불의 20000 입력 → 30000 정정 → 삭제에 대한 사전 예측 SQL과 기대 이벤트.
3. 원천 현재 행·Kafka·Bronze의 before/after·수집 위치·copies를 대조한 경험.
4. 삭제 후 남는 이력, tombstone 처리와 지연 값의 의미, 다음 회차 질문.

학생은 로컬 Docker나 SSH 없이 student-worksheet.sql 하나로 코드 읽기·SELECT 작성·이벤트 예측을 진행한다. 수업 중 submission.md 작성이나 파일 제출은 하지 않으며 수집 결과 메모는 선택이다. 오늘 개인 테이블 DDL·DML은 작성하지 않는다. 수집 시연에서는 Silver·Gold refresh와 Airflow를 실행하지 않는다. 프로젝트 발표는 8회 스터디 뒤 별도 일정이다.

## 이전 실행·자료 검토 기록

아래는 해당 날짜의 기록이며 현재 활동 지시가 아니다. 현재 수업은 위 실습 가이드를 따른다.

- [2026-10-17 자료 검토 반영](review-response.md)
- [2026-10-18 앞선 디스크 정리 후 점검](classcheck-20261018.md)
- [2026-10-17 실제 수집 검증](collection-rehearsal-20261017.md)
- [2026-10-15 수집 검증](collection-rehearsal.md)
- [이전 마트 중심 리허설](rehearsal.md)
