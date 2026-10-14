# 1회차 제출 — 이름 / 학생 ID

## 구성 요소별 책임

| 구성 요소 | 입력·출력과 역할 |
|---|---|
| PostgreSQL | |
| Debezium/Kafka Connect | |
| Kafka | |
| Flink | |
| Iceberg Bronze | |
| Catalog / 객체 저장소 / 복구 상태 | |
| 이후 회차의 Spark·dbt | |

## 원천·Kafka·Bronze 수집 검증

관찰한 CASE_ID:

관찰 방식: 실제 강사 시연 / 과거 리허설 기록 (선택)

| 단계 | 예상 원천 행·이벤트 | 실제 원천 행 수·값 | Kafka key·op·before/after | topic/partition/offset | Bronze의 동일 위치·내용 | 판정과 근거 |
|---|---|---|---|---|---|---|
| INSERT | | | | | | |
| UPDATE | | | | | | |
| DELETE | | | | | | |

- 삭제 후 Bronze에 남은 이력:
- null tombstone의 관찰 여부와 처리 방식:
- 정상 변경 이력과 중복 이벤트를 구분하는 기준:
- 오늘 확인한 범위와 아직 확인하지 못한 것:

### 지연 관찰

| 변경 | 원천 SQL 실행/커밋 관찰 시각 | Kafka 확인 시각 | Bronze 첫 확인 시각 | 대기 명령 관찰 시간 | 측정 한계 |
|---|---|---|---|---|---|
| INSERT | | | | | |
| UPDATE | | | | | |
| DELETE | | | | | |

원천의 정확한 commit 시각을 기록하지 못했다면 추정값을 적지 않습니다. wait 출력은 대기 명령 시작 후 관찰 시간입니다.

## 개인 프로젝트 원천 명세

- 프로젝트 이름 / 도메인:
- 수집 목적과 데이터 소비자:
- 수집 범위와 제외할 데이터:
- 원천 변경부터 Bronze 조회까지 목표 지연 / 이유:
- 원천·이력 보존 계획 / 미정 사항:

| 테이블 | 한 행의 의미 | PK | FK / 관계 | 수정·삭제 컬럼 | 예상 토픽 / 포함 범위 |
|---|---|---|---|---|---|
| | | | | | |
| | | | | | |
| | | | | | |

## 파이프라인 초안

[작성 틀](architecture.md#개인-프로젝트-작성-틀)을 참고해 원천부터 Bronze까지 경로와 각 단계의 입력·출력을 적습니다. 이후 Silver·Gold의 활용은 목적만 표시합니다.

- 원천 / connector / Kafka / Flink / Bronze 경로:
- 저장 파일 / Catalog / checkpoint·savepoint 위치:
- publication·include list·토픽 패턴을 맞출 계획:
- Bronze에 보관할 원문·수집 메타데이터:
- 최신 상태를 만들 때 사용할 원천 키·버전 정보:
- 다음 회차에서 확인할 설정:

## 변경 테스트 계획

| 상황 | 원천 변경 SQL / 대상 PK | 원천 기대 행 | Kafka 기대 key·op·before/after | Bronze에서 찾을 위치·내용 |
|---|---|---|---|---|
| INSERT | | | | |
| UPDATE | | | | |
| DELETE | | | | |

- 동일 Kafka 위치가 Bronze에 두 번 보일 때 판단:
- 같은 내용이 다른 offset에 도착할 때 판단:
- Kafka에 있으나 Bronze에 없을 때 조사 순서:
- 물리 삭제와 논리 삭제를 구분하는 방법:

## 제출 SQL과 다음 회차 준비

- 테이블 하나의 DDL / DML / 원천 SELECT: student-worksheet.sql에 작성.
- 나머지 원천 DDL과 변경 테스트의 기대 이벤트:
- 강사에게 받고 싶은 피드백:

## 종료 질문

1. 변경이 실제로 수집됐다는 근거는 무엇인가?
2. 원천에서 삭제한 행의 이벤트가 Bronze에 남는 이유는?
3. 내 프로젝트의 동일 대상을 어떤 키로 식별하고 어떻게 수집을 검증할 것인가?
