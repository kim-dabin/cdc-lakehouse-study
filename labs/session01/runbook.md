# 1회차 강사용 실행 순서 — CDC 수집 검증

모든 실행 명령은 **맥미니의 프로젝트 폴더**에서 사용한다. 학생은 로컬에서 공통 예제 SQL·이벤트 예측과 수집 관찰 기록을 작성하며 학생별 파일을 서버에서 실행하지 않는다. 개인 설계는 3회차에 확정한다. 핵심 시연은 같은 환불 PK의 INSERT·UPDATE·DELETE를 원천, Kafka, Iceberg Bronze에서 대조한다. Silver·Gold 갱신은 이번 핵심 시연에서 실행하지 않는다.

[수정 큐시트 재실행·재기동 확인 기록](cuesheet-rerun-20261018.md). 앞선 실행은 [2026-10-18 점검 기록](classcheck-20261018.md)에 보관한다.

## 최종 강의안과 연결

현재 [120분 진행안](instructor-guide.md)에 따라 01:10–01:25 환경·코드를 확인하고, 01:25–01:35 준비 데이터·사전 예측, 01:35–01:55 환불 c/u/d 대조를 진행한다. 관찰 결과는 [제출 양식](submission-template.md) 4절에 기록한다. 학생 SQL은 [A] 코드 읽기, [B] 원천 SELECT, [C] 이벤트 예측, [D] Bronze 조회 참고로 구성한다.

## 실행 환경과 포트

구성 파일은 `compose.yaml`이다. 핵심 상시 서비스는 9개이며 Spark와 runner는 별개다. `minio-init`은 버킷 생성 후 종료되는 일회성 서비스이고 Airflow는 선택 프로필로 1회차에서는 정지한다.

| 서비스 | 컨테이너 내부 | 맥미니 호스트 127.0.0.1 |
|---|---|---|
| source-db | source-db:5432 | 25432 |
| catalog-db | catalog-db:5432 | 공개 없음 |
| kafka | kafka:9092 | 공개 없음 |
| connect | connect:8083 | 28083 |
| minio | minio:9000 / Console 9001 | API 29000 / Console 29001 |
| jobmanager | jobmanager:8081 | 28081 |
| taskmanager | 내부 통신 | 공개 없음 |
| spark | Thrift 10000 / UI 4040 | Thrift 21000 / UI 24040 |
| runner | Python·dbt 실행 | 공개 없음 |

강사 노트북에서 아래 SSH 터널을 열어야 노트북의 localhost:28081로 맥미니 Flink UI에 접속할 수 있다. 학생의 서버 접속은 필요하지 않다.

## 수업 전 준비

전날 30–45분을 따로 확보한다. 학생에게 저장소 읽기 권한 또는 두 템플릿을 제공하고 새 CASE_ID로 아래 전체 순서를 리허설한다.

```bash
ssh mini
cd ~/bootcamp/cdc-lakehouse-study
./scripts/lab.sh --profile orchestration stop airflow
./scripts/bootstrap.sh --no-build
./scripts/lab.sh exec -T runner python labs/session01/preflight.py
```

첫 설치·이미지 변경은 수업 전에 `./scripts/bootstrap.sh --build`로 완료한다. 수업 당일 `--no-build`는 로컬 이미지만 사용하며 빌드·이미지 pull을 하지 않는다. 이미지가 없으면 중단하므로 당일 새로 설치하지 않는다.

`ready: true`와 실제 조회를 확인한다. VM 최소 여유 2GiB, 5GiB 이상 권장. bootstrap은 기존 복구 지점에서 시작하며 준비 과정에서 pipeline.py도 실행한다. 이 준비 동작과 수업 중 c/u/d 수집 시연을 구분한다. 초기 설치는 120분에 포함하지 않는다. 복구 지점 없이 작업이 사라졌다면 임의로 `.runtime/flink-state.json`을 삭제하지 않는다.

preflight는 현재 작업의 모든 태스크가 RUNNING이고 체크포인트가 최소 한 번 완료될 때까지 최대 45초 기다린 뒤, 데이터 조회 중 새 체크포인트 실패가 없었는지 확인한다. `checkpoint_failures_during_check: 0`과 `latest_completed_checkpoint_id`도 확인한다. 기동 직후 태스크 초기화 중 취소된 체크포인트는 누적 `checkpoint_counts.failed`에 남을 수 있다. 누적 숫자를 지우거나 무조건 무시하지 말고 최근 완료 기록과 실패 증가 여부를 함께 본다. 완료 기록이 없거나 점검 중 실패가 늘면 시연을 시작하지 않는다. 누적 통계의 의미는 [Flink 1.20 Checkpoint Monitoring](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/monitoring/checkpoint_monitoring/), 응답 항목은 [Flink 1.20 REST API](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/rest_api/)를 참고한다.

강사 노트북에서 Flink 화면을 보여주려면 별도 터미널에서 아래 터널을 열고 `http://localhost:28081`로 접속한다.

```bash
ssh -N -L 28081:127.0.0.1:28081 mini
```

화면 공유에는 `.env`, 비밀번호, 전체 connector 설정을 띄우지 않는다. SQL과 아래 필터링된 조회 출력만 공유한다.

## 1. 새 실행 키와 준비 행

```bash
CASE_ID=s01-live-$(date +%m%d%H%M%S)
cat labs/session01/sql/01-payment.sql
./labs/session01/lab.sh "$CASE_ID" payment
./labs/session01/lab.sh "$CASE_ID" wait --event payment --evidence
```

CASE_ID는 매 수업·리허설마다 새로 만든다. 허용 형식은 `s01-`과 소문자·숫자·하이픈이며 같은 초에 다시 실행할 때도 기존 키와 겹치지 않게 한다. 주문 `CASE_ID-o1`, 결제 `CASE_ID-p1`, 환불 `CASE_ID-r1`을 사용한다. 준비 단계에는 주문·결제가 생기며 핵심 비교에서는 환불 토픽과 키만 필터링한다. 기존 키를 지워 초기화하지 않는다.

원천 커밋 직후에는 아직 Kafka에 이벤트가 없어 빈 목록이 나올 수 있다. 수업에서는 아래처럼 wait 뒤 Kafka 원문을 조회해 최신 op를 보여준다. 강의안에 축약된 명령이 있어도 실제 시연은 이 실행 순서를 따른다.

## 2. INSERT: 원천 한 행과 op=c

```bash
cat labs/session01/sql/02-refund.sql
./labs/session01/lab.sh "$CASE_ID" refund
./labs/session01/lab.sh "$CASE_ID" source-rows
./labs/session01/lab.sh "$CASE_ID" wait --event refund --evidence
./labs/session01/lab.sh "$CASE_ID" kafka
./labs/session01/lab.sh "$CASE_ID" collect-check --stage insert --summary --evidence
```

**요약 출력 화면을 최소 60초 고정**하고 학생이 실제 before/after·offset·bronze_copies를 기록하게 한다. 원천 같은 refund_id 한 행 amount=20000. Kafka key의 refund_id 일치, op=c, before=null, after.amount=20000. Bronze에 같은 topic/partition/offset과 이벤트 내용. 요약의 op=c, bronze_copies=1, passed=True를 확인한다. 전체 checks는 증거 JSON에 남는다.

## 3. UPDATE: 같은 키와 before/after

```bash
cat labs/session01/sql/03-correction.sql
./labs/session01/lab.sh "$CASE_ID" correction
./labs/session01/lab.sh "$CASE_ID" source-rows
./labs/session01/lab.sh "$CASE_ID" wait --event correction --evidence
./labs/session01/lab.sh "$CASE_ID" kafka
./labs/session01/lab.sh "$CASE_ID" collect-check --stage update --summary --evidence
```

**요약 화면을 최소 60초 고정**한다. 원천 같은 PK 한 행 amount=30000. Kafka op=u, before.amount=20000, after.amount=30000. Bronze에는 c와 u 두 이벤트가 서로 다른 수집 위치로 남는다. 원천 행 수와 이력 행 수가 다름을 설명한다.

## 4. DELETE: 원천 0행과 남은 이력

파일 이름 `04-delete-optional.sql`은 기존 이름을 유지하지만, 수정된 1회차에서는 필수 시연이다.

```bash
cat labs/session01/sql/04-delete-optional.sql
./labs/session01/lab.sh "$CASE_ID" delete
./labs/session01/lab.sh "$CASE_ID" source-rows
./labs/session01/lab.sh "$CASE_ID" wait --event delete --evidence
./labs/session01/lab.sh "$CASE_ID" kafka
./labs/session01/lab.sh "$CASE_ID" collect-check --stage delete --summary --evidence
```

**요약 화면을 최소 60초 고정**하고 마지막 지연 한 세트까지 기록하게 한다. 원천 해당 PK는 0행. Kafka op=d, before.amount=30000, after=null. Bronze에는 c/u/d 세 이벤트가 남는다. Kafka key/value와 null tombstone을 구분한다. Flink 원문 적재 SQL은 tombstone null을 제외하므로 tombstone까지 포함한 Kafka 메시지 수와 Bronze 행 수가 다를 수 있다.

`kafka`와 `collect-check`는 독립적인 읽기 전용 소비자를 사용하며 Flink consumer group의 offset을 변경하지 않는다. 환불 토픽의 시작부터 호출 시점 end offset까지 읽어 이번 key만 출력하고 스캔은 30초로 제한한다. 오래 쌓인 실습 환경에서 timeout이 나면 수업 중 전체 Kafka 데이터를 삭제하지 말고 리허설 기록으로 전환한다.

## 5. 대조 결과와 지연 설명

`collect-check`가 검사하는 범위:

- 해당 단계까지 예상하는 c/u/d와 before/after.
- 원천 환불 행의 현재 값 또는 삭제 상태.
- Kafka key와 원천 PK.
- 동일 (topic,partition,offset)의 Bronze 내용·source.lsn.
- 이번 키의 non-null Kafka 이벤트와 Bronze 행 수. 중복이나 예상 밖 이벤트는 실패로 표시.

자동 검사는 이번 합성 키의 기대 시나리오만 확인한다. 모든 데이터의 정합성이나 exactly-once·장애 복구를 보장하는 검사라고 말하지 않는다.

wait --evidence는 wait-EVENT.json에 대기 시작·관찰 시각과 관찰 시간을 저장한다. collect-check가 이를 wait_observation으로 연결한다. 기록이 없으면 null이다. 요약은 해당 단계 이벤트만 보여주고 --evidence에는 전체 대조 자료와 -summary.json을 함께 보관한다.

wait 출력은 대기 시작 이후 첫 Bronze 관찰 시간이다. `kafka_ts`와 `ingested_at` 차이는 전체 commit 지연이 아니다. 학생은 DELETE 요약의 kafka_ts, ingested_at, wait_observed_seconds 한 세트만 기록하고 의미를 구분한다. 원천 commit이나 Iceberg commit 시각을 이 값으로 대신하지 않는다.

증거는 `artifacts/runs/session01/CASE_ID/collection-insert.json`, `collection-update.json`, `collection-delete.json`에 저장된다. 값 비교가 완료된 뒤 필요한 증거만 수업 자료로 복사한다.

## 문제가 생길 때

| 증상 | 확인할 곳 / 진행 |
|---|---|
| PK 충돌 | 새 CASE_ID 사용. 기존 시연 데이터를 지우지 않음 |
| 원천만 바뀜 | commit, publication·include list, connector 상태 확인 |
| Kafka에만 보임 | Flink 입력 토픽, 처리·checkpoint, sink commit, Catalog·MinIO 접근 확인 |
| wait timeout / Kafka 스캔 timeout | 실패한 구간을 기록. 5분 넘으면 과거 리허설임을 명시하고 자료로 전환 |
| 예상 밖 이벤트 / 중복 | 위치와 내용을 보존. 새 키로 핵심 시나리오를 다시 확인 |
| Silver·Gold는 과거 값 | 핵심 CDC 수집 시연과 별도. 오늘 refresh를 실행하지 않음 |

학생은 제출 양식 4절에 조사할 구간을 적고, 과거 리허설로 전환했다면 관찰 방식을 표시한다. 같은 장애를 20분 동안 공개 디버깅하지 않고 종료·제출 시간을 확보한다.

## 수업 종료

```bash
./scripts/stop.sh
```

이 프로젝트만 savepoint와 함께 종료한다. 맥미니 전체 VM이나 다른 데모를 중단하지 않는다. 원천·Kafka·Iceberg 데이터는 보존한다.
