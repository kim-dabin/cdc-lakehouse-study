# 2회차 강사용 진행안 — 변경 데이터가 수집되는 조건을 확인하기

강사는 맥미니에서 공통 예제를 실행한다. 학생은 SQL과 이벤트 결과를 먼저 예측하고, 서로 진행한 내용을 짧게 나눈 뒤 강사 화면의 실제 결과와 비교한다. 개인 환경 구성과 공통 예제 재현은 수업 후 복습으로 진행한다. 수업 중 별도 제출 양식을 작성하거나 개인 테이블을 설계하지 않는다.

이번 시간에는 1회차에서 이어 보기로 했던 **Bronze의 c/u/d 세 이력과 실제 Parquet 내용**부터 확인한다. 그다음 PostgreSQL → Debezium → Kafka의 설정과 이벤트를 따라가고, 작은 Python 파서로 메시지를 해석한다. 개인 설계 확정은 3회차, Silver·Gold 변환과 dbt 구현은 이후 회차다.

## 오늘 확인할 것

1. 원천에서 삭제된 환불이 Bronze에 어떤 이력으로 남았는지 SQL과 실제 파일로 설명한다.
2. 수집 대상, 이전 값, 복구 위치가 각각 어느 설정에서 결정되는지 찾는다.
3. Kafka의 key/value를 읽고 DELETE와 tombstone, 업무 PK와 수집 위치를 구분한다.
4. 한 구간에서 데이터가 보이지 않을 때 다음으로 확인할 곳을 근거와 함께 제시한다.

## 120분 시간표

| 경과 시간 | 분 | 강사 진행 | 학생 활동 |
|---|---:|---|---|
| 00:00–00:10 | 10 | 복습 진행 공유·오늘 확인할 질문 | 진행한 지점과 막힌 구간을 짧게 공유 |
| 00:10–00:30 | 20 | 원천 0행·Bronze c/u/d 세 이력·실제 Parquet 내용 | DELETE 뒤 조회 조건과 파일 내용을 예측·대조 |
| 00:30–00:50 | 20 | PostgreSQL 수집 조건과 Debezium 설정 | 현재 수집 범위·이전 값·복구 위치의 조건 찾기 |
| 00:50–00:55 | 5 | 휴식 | 5분 휴식 |
| 00:55–01:15 | 20 | snapshot r 비교·독립 파서 코드·예시 입력·SQL 연결 | 입력 결과 예측 후 실행·조건 비교 |
| 01:15–01:35 | 20 | 새 키의 20000→30000→DELETE와 Kafka·Bronze·파서 대조 | 각 단계 예측 후 실제 값 비교 |
| 01:35–01:50 | 15 | 수집 구간별 진단 연습 | 다음 확인 지점과 이유 설명 |
| 01:50–02:00 | 10 | 정리·질문·수업 후 복습·3회차 연결 | 남은 질문과 개인 도메인 관심사 공유 |

실행 명령과 읽을 멘트는 [강사 큐시트](cuesheet.md), 설정 의미는 [공식 참고 문서](references.md)를 사용한다. 설치·이미지 빌드·접속 문제 해결은 수업 전에 끝낸다.

## 수업 전 준비

- 현재 수업용 키와 원천·Kafka·Bronze 결과를 먼저 확인한다. 이전 수업 이력이 남아 있으면 그대로 사용하고, 새 예시가 필요하면 기존 공통 예제 helper에 새 키를 사용한다. 같은 키를 지우거나 덮어써서 수업을 초기화하지 않는다.
- 준비된 이미지로 기동하고 preflight를 확인한다. RUNNING 표시 외에 최근 완료 checkpoint, 점검 중 추가 실패 여부와 실제 조회까지 본다. 준비용 bootstrap에서 실행되는 변환과 오늘 Bronze 조회를 구분한다.
- Bronze에서 조회한 이벤트를 현재 파일 경로와 연결하고 Parquet를 미리 읽어 둔다. 수업에서 파일명이나 비밀번호를 찾느라 멈추지 않도록 한다. 실제 파일 내용을 보여줄 경로는 큐시트의 확인 명령 결과를 사용한다.
- 비밀번호가 치환된 SQL, `.env`, 전체 connector 설정을 화면에 띄우지 않는다. 설정은 수업에 필요한 항목만 출력한다.
- 예시 입력과 리허설 결과는 현재 실측과 구분해서 보관한다. 실패 시 5분 안에 확인할 항목과 전환할 자료를 정한다.

## 00:00–00:10 — 진행 공유와 오늘의 질문

처음 5분은 두 사람 정도에게 진행 상황을 듣는다. 성공 화면을 전부 발표시키기보다 “어디까지 했는지, 무엇이 보였는지, 어느 구간에서 막혔는지”를 한 가지씩 묻는다. 개인 계정이나 비밀번호가 보이는 화면은 공유하지 않도록 안내한다.

남은 5분에는 지난 시간의 환불 시나리오를 다시 꺼낸다.

> “원천에서 환불 행을 지웠는데도 Bronze에는 c, u, d가 남는다고 했죠. 오늘은 그 말을 결과 요약이 아니라 SELECT와 실제 파일 내용으로 확인하겠습니다.”

학생에게 원천 행 수와 Bronze 이력 수가 왜 다른지 먼저 묻는다. 아직 성공하지 못한 학생도 예상 결과를 말할 수 있게 하고, 환경 문제는 수업 중 공개 디버깅으로 길게 끌지 않는다.

## 00:10–00:30 — 남아 있는 이력과 파일 직접 확인

**10–18분:** 같은 환불 ID로 원천과 Bronze를 조회한다. DELETE는 `after`가 null이므로 before에서도 ID를 찾아야 한다. 수집 위치를 정렬해 c: null→20000, u: 20000→30000, d: 30000→null을 나란히 읽는다. 이번 키의 세 이력을 보는 것이며 전체 Bronze가 세 행이라는 뜻이 아니다.

**18–27분:** 현재 Iceberg 파일 경로를 확인한 뒤 그 경로의 Parquet 내용을 읽는다. 학생에게 “테이블 하나는 파일 하나일까요?”, “지금 연 파일에 세 이벤트가 모두 있을까요?”를 묻는다. 여러 파일에 나뉘어 있다면 실제 관찰대로 설명한다. 메타데이터 파일을 보여주는 데서 끝내지 않고 데이터 행을 확인한다. helper는 S3FileIO로 실제 바이트를 읽고 GenericParquetReaders로 디코드한다. `sql_file_records_match`와 `three_event_history_complete`까지 확인한다. [Iceberg 파일 조회](https://iceberg.apache.org/docs/1.10.0/spark-queries/), [파일 디코드 API](https://iceberg.apache.org/javadoc/1.10.0/org/apache/iceberg/data/parquet/GenericParquetReaders.html).

**27–30분:** 원천 현재 행, Kafka 메시지, Bronze 이력, 물리 파일을 다시 연결한다. Parquet 직접 읽기는 내용 확인이며 모든 파일을 합쳐 현재 Iceberg 상태를 계산하는 일반 방법은 아니라고 짧게 구분한다.

## 00:30–00:50 — PostgreSQL과 Debezium의 수집 조건

**30–36분:** `wal_level`, publication과 포함된 테이블, refunds의 replica identity를 읽는다. FULL은 이전 금액을 관찰하기 위한 예제 조건이다. 수업 중 FULL을 껐다 켜며 기존 수집을 바꾸지는 않는다. [Publication](https://www.postgresql.org/docs/17/sql-createpublication.html), [Replica identity](https://www.postgresql.org/docs/17/sql-altertable.html).

**36–42분:** slot 상태에서 active, restart_lsn, confirmed_flush_lsn을 확인한다. 슬롯은 연결이 끊겨도 남고 WAL 보존 비용이 생길 수 있다. 숫자가 커지는 것을 성공의 전부로 삼거나 LSN 차이를 지연 초로 해석하지 않는다. 기존 슬롯을 소비하거나 진행시키는 함수는 실행하지 않는다. [Logical decoding](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html), [slot 상태](https://www.postgresql.org/docs/17/view-pg-replication-slots.html).

**42–48분:** 비밀값을 제외한 Debezium 설정을 읽는다. `pgoutput`, slot·publication 이름, include list, topic prefix, snapshot mode, tombstone 설정을 원천의 실제 구성과 연결한다. [Debezium 3.2](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html).

**48–50분:** “publication에는 있는데 include list에는 없다면?”, “원천은 바뀌었는데 이벤트가 없다면 어떤 조건부터 볼까요?”를 묻는다. 현재 코드를 근거로 답하고, 장애를 만들려고 설정을 바꾸지는 않는다.

## 00:55–01:15 — snapshot과 작은 파서로 해석 연습

**55–60분:** r과 c를 비교한다. `snapshot.mode=initial`인 기존 환경을 정상 재시작한다고 항상 r이 다시 생기는 것은 아니다. `snapshot-sample`은 현재 Bronze에 보존된 r을 조회하며 `origin=live_bronze`와 `sample_found`를 확인한다. 표본이 없으면 독립 fixture로 설명하고 오늘 새로 관찰한 이벤트처럼 말하지 않는다. 스냅샷을 재현하려고 connector나 offset을 지우지 않는다.

**60–67분:** 독립 Python 파서가 받는 입력과 내보내는 필드를 먼저 보여준다. 이미 돌아가는 Kafka Connect나 Flink 코드를 교체하는 것이 아니라 이벤트 해석을 연습하는 함수다. 업무 PK, `source.lsn`, `(topic, partition, offset)`의 역할을 구분한다. offset 순서는 같은 토픽·파티션 안에서만 비교한다. [Kafka 파티션](https://kafka.apache.org/39/design/protocol/).

**67–72분:** c/u/d와 tombstone, r, 잘못된 입력을 먼저 예측한 뒤 fixture로 확인한다. DELETE는 before에서 ID를 찾고, value 전체가 null인 tombstone은 `after=null`인 삭제 객체와 별도로 처리한다. 해석할 수 없는 입력을 정상 행으로 꾸며내지 않는 점도 본다. 예시 입력의 출처를 실제 Kafka 조회와 구분한다. JSON null과 Python None의 출력 차이는 여기서 정리한다. [Python json](https://docs.python.org/3.11/library/json.html).

**72–75분:** 같은 선택을 SQL로 옮긴다. `after.refund_id`만 보는 조건과 `before.refund_id`까지 보는 조건을 비교한다. 답이 다르면 DELETE 입력 한 건으로 다시 설명하게 한다. 이 SQL은 정상 이벤트의 값 선택을 비교하는 예이며 파서의 모든 오류 검사를 재구현하지 않는다. Silver 최신 상태를 만드는 MERGE나 중복 제거 구현으로 확장하지 않는다.

## 01:15–01:35 — 새 변경을 예측하고 실제 값 대조

**75–77분:** 새 CASE_ID를 만든다. 앞에서 읽은 1회차 키와 구분하고 주문·결제 준비가 도착했는지 확인한다. 기존 키를 지우거나 재사용하지 않는다.

**77–82분 INSERT / 82–87분 UPDATE / 87–93분 DELETE:** 실행 전에 원천 행 수·op·before/after를 묻는다. 각 단계에서 DML → 원천 조회 → 도착 대기 → Kafka → Bronze → 실제 Kafka 입력의 파서 출력을 대조한다. 원천 현재 행과 append 이력이 어떻게 다른지 연결한다. 같은 PK의 여러 이벤트를 곧바로 중복 수집이라고 부르지 않는다.

**93–95분:** DELETE 뒤 이번 키의 c/u/d 세 이력과 tombstone 분기를 정리한다. 자동 대조 결과는 이 키의 합성 시나리오 범위임을 설명한다. 파서에서 `origin=live_kafka`를 확인하고 앞의 `synthetic_fixture`와 구분한다. 예측이 달랐던 부분 한 가지를 학생에게 묻는다.

## 01:35–01:50 — 어디부터 확인할지 판단하기

다음 세 상황을 차례로 제시한다. 실제 장애를 일으키지 않고 관찰 결과를 근거로 토의한다.

| 관찰 | 먼저 제안할 확인 지점 | 피할 단정 |
|---|---|---|
| 원천은 바뀌었는데 Kafka에 해당 키가 안 보임 | commit·수집 대상·connector/task·조회 범위 | “CDC가 데이터를 잃었다” |
| Kafka에 있지만 Bronze에 안 보임 | Flink 입력 범위·처리·checkpoint·sink commit·조회 조건 | “checkpoint 성공이니 적재도 끝났다” |
| Bronze에 d가 없다고 보임 | after만 검색했는지, 실제 키와 토픽·파티션·offset | “삭제 이벤트는 저장하지 않는다” |

학생이 확인 순서와 이유를 말하게 하고, 강사는 조회 결과와 연결해 보완한다. connector와 task의 RUNNING은 상태 정보이며 해당 이벤트의 도착 증거를 대신하지 않는다. [Kafka Connect 상태](https://kafka.apache.org/39/kafka-connect/administration/), [Iceberg commit](https://iceberg.apache.org/docs/1.10.0/flink-writes/).

## 01:50–02:00 — 정리와 수업 후 복습

앞의 질문 세 가지로 마무리한다. “삭제된 행의 ID는 어디서 찾았나요?”, “커넥터를 다시 켜면 언제나 r이 생기나요?”, “수집이 막히면 어느 구간의 증거부터 볼까요?”

개인 환경에서 공통 예제를 재현하고 SQL·파서 결과를 비교하는 복습 범위를 안내한다. 실제로 확인하지 못한 결과는 미확인으로 남기고 오류와 진행한 구간을 질문하도록 한다. 제출 방법·기한은 별도 안내를 따르며 여기서 새로 정하지 않는다. 개인 프로젝트는 관심 도메인을 생각해두고 3회차에 원천 테이블 설계를 확정한다.

## 시연이 늦어질 때

원천·Kafka·Bronze 중 어디까지 보였는지 먼저 기록한다. 한 문제에 5분 이상 쓰지 않고, 해당 시각과 키가 표시된 리허설 결과 또는 예시 입력으로 전환한다. 학생에게 과거 기록인지 예시인지 알린다. 초기화·토픽 삭제·offset 변경으로 즉석 복구하지 않는다. Parquet 실물 확인과 이벤트 해석은 핵심이므로 긴 메타데이터 탐색이나 추가 사례부터 줄이고 정리 시간을 남긴다.
