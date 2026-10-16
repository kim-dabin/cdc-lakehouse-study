# 1회차 — 변경 데이터 수집과 파이프라인 설계

**120분 · 온라인 · 강사 맥미니 시연 + 학생 SQL·설계서 작성**

PostgreSQL의 INSERT·UPDATE·DELETE가 Kafka 이벤트로 전달되고 Iceberg Bronze에 남는 과정을 확인한다. 같은 PK의 원천 행과 이벤트 key/op/before/after, 토픽·파티션·오프셋을 대조하고 개인 프로젝트의 원천 명세와 파이프라인을 설계한다.

- [외부 검토 반영과 판단](review-response.md)
- [왜 CDC를 배우는가 / 개인 프로젝트 다섯 가지](why-cdc-and-projects.md)
- [강의용 PPT](slides/README.md)
- [학생 실습 가이드](student-guide.md)
- [강사용 120분 진행안](instructor-guide.md)
- [아키텍처 세 장과 개인 설계 틀](architecture.md)
- [학생 제출 템플릿](submission-template.md)
- [맥미니 시연 명령과 장애 대응](runbook.md)
- [2026-10-17 실제 CDC 수집 검증과 요약 화면](collection-rehearsal-20261017.md)
- [2026-10-15 이전 수집 검증](collection-rehearsal.md)
- [이전 마트 중심 리허설 기록](rehearsal.md)

![데이터 흐름](../../docs/diagrams/01-data-flow.png)

## 수업 후 남는 것

1. 같은 행의 입력·수정·삭제를 원천/Kafka/Bronze에서 대조한 기록.
2. 수집 성공의 근거, 누락·중복을 조사할 구간과 지연 측정의 한계.
3. 개인 핵심 테이블 하나의 명세·DDL·기대 이벤트. 나머지 테이블은 다음 회차 전 과제로 확장.
4. 수집 경로, 저장·메타데이터 관계, 변경 테스트와 검증 계획.

학생 파일은 자동으로 서버에 실행하지 않는다. 신규 테이블의 publication/connector/Flink 연결, 현재 상태 구성과 분석 마트는 이후 회차로 확장한다. 프로젝트 발표는 8회 스터디 이후 별도 일정이다.
