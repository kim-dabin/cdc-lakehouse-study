# 1회차 강사용 진행안 — 120분

최종 강의안의 OT·개념·환경 구성·공통 예제 순서에 맞춘 진행안이다. 강사는 맥미니에서 실행하고 학생은 로컬 SQL을 작성하고 실제 수집 결과를 함께 대조한다. 개인 프로젝트 설계는 3회차에 확정하며 오늘은 공통 예제에 집중한다.

## 시간표와 학생 활동

| 경과 시간 | 분 | 진행 | 학생 행동 |
|---|---:|---|---|
| 00:00–00:12 | 12 | OT·8회 로드맵·오늘 범위 | student-worksheet.sql 준비 |
| 00:12–00:35 | 23 | 왜 CDC 레이크하우스인가 | 필요 상황과 운영 비용 이해 |
| 00:35–01:05 | 30 | CDC·Iceberg 개념, 아키텍처 세 장 | 구성 요소 역할 구분 |
| 01:05–01:10 | 5 | 휴식 | 5분 휴식 |
| 01:10–01:35 | 25 | 저장소·실행 환경·ingest.sql·사전 예측 | SQL [A] 코드 읽기·[B] SELECT 작성·[C] 이벤트 예측 |
| 01:35–01:55 | 20 | 환불 c/u/d 수집 대조 | 예측과 실제 결과 대조, DELETE 지연 확인 |
| 01:55–02:00 | 5 | 정리·질문·2회차 안내 | 오늘 확인한 차이와 질문 나누기 |

설치·이미지 빌드·리허설은 수업 전에 끝낸다. 학생 Docker 설치나 SSH 접속을 진행하지 않는다. 프로젝트 발표는 8회 스터디 뒤 별도 일정이다. 이전 PPT의 시간표·개인 설계 활동을 사용하지 않는다.

## 00:00–00:12 — OT와 8회 로드맵

정규 과정에서 배운 Kafka·Spark·S3·Docker·Airflow를 어떤 흐름으로 연결할지 설명한다. 공통 orders·payments·refunds로 동작을 익히고, 3회차에 개인 도메인·테이블을 확정한 뒤 확장한다.

[학생 SQL](sql/student-worksheet.sql) 하나만 준비시킨다. 수업 중 submission.md 작성이나 파일 제출은 진행하지 않으며, 관찰 메모는 선택이라고 안내한다. 학생 SQL의 my-r1은 예측용 이름이며 실제 실행은 강사 CASE_ID-r1을 사용한다고 안내한다.

## 00:12–00:35 — 왜 CDC 레이크하우스인가

금액 정정·예약 취소·물리 삭제처럼 현재 행이 계속 바뀌는 상황에서 출발한다. 전체 복사·updated_at 조회·로그 기반 CDC의 차이를 비교하고, 변경 보존·짧은 반영 지연·OLTP와 분석 처리 분리의 이유를 설명한다. [CDC 필요성과 프로젝트 예시](why-cdc-and-projects.md)를 보조 자료로 사용한다.

CDC에도 초기 스냅샷 부하·WAL 보존·전달 지연·목적지 커밋·복구 비용이 있다. CDC만 붙이면 이중 쓰기나 순서 문제가 모두 해결된다고 말하지 않는다. 채용공고를 수업의 진행 축으로 삼지 않는다. 학생과 CDC가 필요한 상황과 관리할 비용을 함께 짚는다.

## 00:35–01:05 — CDC·Iceberg와 아키텍처 세 장

[아키텍처 자료](architecture.md)를 사용해 데이터 흐름, 저장·메타데이터 관계, 실행 환경을 구분한다.

- 초기 스냅샷 r과 새 변경 c/u/d, PK와 Kafka key·수집 위치를 구분한다.
- 예제 원천의 REPLICA IDENTITY FULL은 이전 금액 관찰을 위한 조건이다.
- Bronze는 raw value와 수집 위치를 append한다. Kafka key는 별도 Bronze 컬럼에 저장하지 않는다.
- null tombstone만 제외하며 non-null c/u/d와 r은 보관한다.
- Bronze·Silver·Gold는 처리 계층이다. Iceberg 내부 메타데이터 JSON·manifest list·manifest·데이터 파일의 관계와 혼동하지 않는다.
- 파일은 MinIO, 현재 테이블 메타데이터 포인터는 JDBC Catalog, 복구 상태는 Flink 볼륨에 저장한다.
- Spark SQL Silver·dbt Gold는 별도 실행 시 갱신된다. 오늘 수집 시연은 Bronze까지이며 Airflow도 정지한다.

Iceberg의 행 단위 SQL이 기존 Parquet를 제자리에서 수정하는 것처럼 설명하지 않는다. 구성 요소의 역할을 정리하고 01:05에 휴식한다.

## 01:10–01:35 — 환경 확인·코드 읽기·사전 예측

### 01:10–01:25 — 실행 환경과 코드

[실행 안내](runbook.md)에 따라 컨테이너 목록, preflight, Flink UI를 보여준다. 핵심 상시 서비스 9개, Spark와 runner의 분리, 내부·호스트 포트 차이를 설명한다. 학생은 자신의 SQL 편집기와 student-worksheet.sql을 점검한다.

flink/ingest.sql.template에서 Catalog·warehouse, topic-pattern·raw, checkpoint, payload IS NOT NULL 네 곳을 짚는다. 학생 SQL [A]의 질문에 연결한다. 비밀번호가 치환된 런타임 SQL·전체 connector 설정은 공유하지 않는다.

### 01:25–01:35 — 준비 데이터와 사전 예측

새 CASE_ID로 payment 준비 행을 만들고 wait --event payment --evidence까지 확인한다. 원천 모델 2분, 학생 작성 5분, 기대값 확인 3분으로 진행한다. SQL [B]의 두 SELECT와 [C] 이벤트 예측을 작성시킨다.

기대값은 c: null→20000, u:20000→30000, d:30000→null이다. 원천은 차례로 1행·20000, 1행·30000, 0행이다. 개인 테이블·DDL을 새로 작성시키지 않는다.

## 01:35–01:55 — 공통 환불 수집 시연

[실행 안내](runbook.md)의 명령을 사용한다. INSERT 6분, UPDATE 6분, DELETE 8분으로 배분하고 각 단계 요약 화면을 최소 60초 고정한다.

1. 변경 DML 실행.
2. source-rows로 원천 현재 행 확인.
3. wait --event 해당이벤트 --evidence로 Bronze 도착 대기.
4. kafka로 해당 키의 이벤트 확인.
5. collect-check --stage 해당단계 --summary --evidence로 대조 결과 확인.

원천 커밋 직후 Kafka를 조회하면 빈 목록일 수 있으므로 wait 뒤 보여준다. 학생은 자신의 예측과 실제 before/after·offset·bronze_copies를 함께 대조한다. 필요한 내용만 선택해서 메모하게 하고 전체 JSON을 받아쓰게 하지 않는다. None은 JSON null의 출력 표현임을 설명한다. 요약에 없는 전체 checks를 학생에게 찾으라고 하지 않는다.

DELETE 뒤 원천 0행, 이번 키의 Kafka c/u/d와 tombstone, Bronze c/u/d 3건을 구분한다. 이는 전체 테이블 3행이라는 뜻이 아니다. 같은 Kafka 위치의 copies=1과 같은 PK의 여러 변경 이력을 구분한다. source.lsn은 단독 고유 이벤트 ID가 아니고 offset 순서는 토픽·파티션 안에서 비교한다.

DELETE 요약의 kafka_ts·ingested_at·wait_observed_seconds를 보여주며 기준 시점을 함께 확인한다. wait는 명령 시작 기준이며 전체 원천 commit→마트 지연이 아니다. checkpoint 완료 후 sink commit 성공과 실제 조회를 확인해야 한다. RUNNING 또는 checkpoint 성공만으로 완료라고 말하지 않는다.

## 01:55–02:00 — 정리와 다음 회차

SQL [A]·[B]·[C]에서 확인한 내용과 예측·실제 결과의 차이를 함께 정리하고 질문을 받는다. SQL [D]는 제공된 Bronze 조회 참고용이며 별도 실행 과제가 아니다. submission.md를 작성하거나 두 파일을 제출받는 시간은 두지 않는다. [수업 후 복습 양식 — 수업 중 사용 안 함](submission-template.md)을 안내하고, 제출 방법과 기한은 수업 마무리에 설명한다.

2회차는 PostgreSQL·Debezium·Kafka 수집 심화다. 오늘 개인 DDL·DML을 요구하지 않는다. 관심 도메인은 선택 메모만 남기도록 안내하고 설계 확정은 3회차에 진행한다.

## 시연이 지연되면

문제 구간을 기록하고 5분을 넘기면 [오늘 리허설 기록](classcheck-20261018.md)과 그 증거로 전환한다. 학생에게 과거 리허설 기록이라는 점을 안내한다. 과거 값을 새 실측으로 쓰지 않는다. 이미 작성한 예측과 과거 수집 결과를 대조하며 정리·질문 시간을 확보한다. 수업 뒤에는 runbook의 savepoint 종료 절차를 따른다.
