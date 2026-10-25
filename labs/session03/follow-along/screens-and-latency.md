# 3회차 따라하기 — 화면에서 확인하고 새 환불의 지연 재기

[SQL 따라하기](README.md)에서 자신의 `CASE_ID`로 환불 생성·정정·삭제를 끝낸 뒤 이어갑니다. 같은 저장소 루트의 같은 Bash 터미널을 사용합니다. 영상 속 키·offset·snapshot 번호를 복사하지 않고 **자기 실행의 값**을 확인합니다.

오늘 연결할 경로는 **Kafka 이벤트 → Flink 처리·checkpoint → Iceberg 테이블 확정 → 같은 키 조회 → 실제 파일**입니다. 웹 화면은 개인 Docker 환경을 실행한 컴퓨터의 브라우저에서 엽니다. 강사 서버 접속이나 SSH 터널은 필요하지 않습니다.

## 7. Kafka UI에서 c·u·d와 tombstone 보기

기본 환경이 준비된 다음 읽기 전용 관찰 화면을 추가합니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml \
  up -d --no-deps kafka-ui
printf '검색할 내 환불 ID: %s-r1\n' "$CASE_ID"
```

`compose.observers.yaml`은 Kafka UI를 추가하는 설정입니다. 위 명령은 이미 실행 중인 원천·Kafka·Flink를 다시 만들거나 데이터를 입력하는 명령이 아닙니다.

브라우저에서 **http://localhost:28082**를 엽니다.

1. Dashboard에서 `cdc-study` 클러스터를 확인하고 **Topics → cdc.public.refunds → Messages**로 이동합니다.
2. Search에 방금 출력한 **자신의 환불 ID 전체**를 붙여넣고 **Enter**를 누릅니다. 검색 조건이 반영됐는지 확인한 뒤 **Refresh**합니다.
3. 같은 키의 레코드를 펼쳐 Key·Value·partition·offset을 읽습니다. 화면에 보이는 정렬 순서와 무관하게 같은 토픽·파티션의 offset으로 순서를 비교합니다.

각 변경 SQL을 한 번씩 실행한 이번 예제의 기대값입니다.

| 레코드 | Value에서 볼 것 |
|---|---|
| c | before=null, after.amount=20000 |
| u | before.amount=20000, after.amount=30000 |
| d | before.amount=30000, after=null인 JSON 본문 |
| tombstone | 같은 키, value 전체가 null인 별도 레코드 |

Kafka에는 누적4건, Bronze에는 본문이 있는 c/u/d 3행이 남습니다. d의 `after:null`과 tombstone의 **value 전체 null**을 구분합니다. Kafka UI의 빈 화면만으로 수집 누락이라고 판단하지 말고 검색 키·토픽·검색 범위를 먼저 확인합니다. 터미널의 같은 키 결과도 대조합니다.

```bash
./labs/session02/lab.sh "$CASE_ID" kafka --summary --evidence
```

UI에 접속되지 않으면 원천 DML을 다시 실행하지 않고 관찰 서비스 상태부터 봅니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml ps kafka-ui
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml \
  logs --tail 60 kafka-ui
```

**멈춤 기준:** 자기 키의 c/u/d·tombstone을 구분하고, CLI와 topic·partition·offset이 일치하면 계속합니다.

## 8. Kafka 커밋과 Flink checkpoint 구분하기

Kafka UI의 **Consumers → flink-cdc-bronze**에서 refunds 토픽의 파티션별 committed offset·lag를 봅니다.

- Messages의 offset은 **레코드 위치**입니다.
- consumer의 committed offset은 **다음 읽을 위치**입니다.
- lag=0은 그 위치 기준으로 소비가 따라왔다는 뜻이지, 같은 환불의 Iceberg 조회 지연이0초라는 뜻은 아닙니다.

이제 **http://localhost:28081 → Running Jobs → cdc-bronze → Checkpoints**를 엽니다. Completed·Failed와 최근 완료 시각을 확인하고 새 checkpoint가 진행하는지 봅니다. 과거 누적 실패 수가 있다면 최근 증가 여부와 오류를 함께 확인합니다.

Flink는 checkpoint에 저장한 입력 위치와 상태를 복원해 이어서 처리합니다. Kafka의 consumer 커밋 위치만을 복구 기준으로 삼는 것과 구분합니다. 예제 기본 설정의10초는 checkpoint 시도 간격이며 **완료 시간이나 Bronze 조회 지연의 상한을 보장하지 않습니다.**

checkpoint Completed만으로 Iceberg commit 성공을 단정하지 않습니다. checkpoint ID와 snapshot ID를 같은 번호로 맞추지도 않습니다.

**멈춤 기준:** Kafka의 소비 위치와 Flink의 복구 지점이 각각 무엇을 확인하는지 한 문장으로 설명합니다.

## 9. 실행 중인 Flink SQL 읽기

```bash
cat flink/ingest.sql.template
```

파일을 **읽기만** 합니다. SQL Client에 새로 제출하지 않습니다. 아래 부분을 찾아봅니다.

```sql
INSERT INTO lake.bronze.cdc_events
SELECT payload, topic, partition_id, kafka_offset, kafka_ts, CURRENT_TIMESTAMP
FROM kafka_events
WHERE payload IS NOT NULL;
```

Kafka raw 본문을 `payload` 문자열로 읽고 전달 위치·시각과 함께 이력으로 추가합니다. null 필터는 value 전체가 없는 tombstone을 제외하며, d 본문은 일반 행으로 보존합니다. 이 SQL은 업무 JOIN·집계·현재 상태 upsert·업무 중복 제거를 하지 않습니다.

이번 sink의 흐름은 **행을 파일에 쓰기 → checkpoint 준비 때 파일 마감·상태 기록 → 전체 checkpoint 완료 통지 → Iceberg commit**입니다. checkpoint 전까지 모든 행을 메모리에만 모으는 구조는 아닙니다. 파일 작성·checkpoint 성공·테이블 확정은 서로 다른 경계입니다.

## 10. Catalog 주소에서 MinIO JSON으로 따라가기

먼저 **별도 Catalog PostgreSQL**을 조회합니다. 원천 source-db가 아니라 `catalog-db`입니다.

```bash
date
./scripts/lab.sh exec -T catalog-db psql -X -v ON_ERROR_STOP=1 \
  -U catalog -d catalog -c "
SELECT metadata_location FROM iceberg_tables
WHERE table_namespace='bronze' AND table_name='cdc_events';"
```

`metadata_location` 한 행과 조회 시각을 메모합니다. 여러 행이 나오면 catalog 구분을 확인하고 임의로 하나를 고르지 않습니다. 주소는 `s3://warehouse/bronze/cdc_events/metadata/...metadata.json` 형태이며 실제 파일명은 실행마다 달라집니다.

브라우저에서 **http://localhost:29001**을 엽니다. 자신의 로컬 `.env`에서 `S3_ACCESS_KEY`·`S3_SECRET_KEY` 값을 확인해 로그인합니다. 이는 격리 실습용 계정이며 실제 AWS 계정이나 회사 키를 입력하는 곳이 아닙니다.

1. **Object Browser → warehouse → bronze/cdc_events/metadata/**로 이동합니다.
2. Catalog에서 방금 읽은 파일명으로 검색해 **같은 객체**를 찾습니다. 가장 큰 번호나 가장 최근 수정 시각만으로 고르지 않습니다.
3. ObjectInfo의 Name·Size·Last Modified를 보고 **Download**합니다. JSON Preview가 지원되지 않거나 MIME type 안내가 나오면 Download한 원본으로 진행합니다.
4. 다운로드 폴더에서 그 JSON을 텍스트 편집기로 엽니다. 파일을 마우스 오른쪽 버튼으로 눌러 텍스트 편집기를 선택하거나, 편집기의 파일 열기로 내려받은 원본을 선택합니다.
5. **Ctrl+F, Mac은 ⌘F**로 `current-snapshot-id`를 찾습니다. 긴 정수는 자리수를 그대로 읽고 반올림하지 않습니다.
6. 같은 ID의 snapshot 항목에서 `manifest-list` 주소를 찾습니다. metadata JSON에서 파일 목록으로 이어지는 참조입니다. manifest list·manifest 자체의 형식은 **Avro**이며 JSON이나 Parquet와 다릅니다.

다운로드 시각과 파일명을 함께 남깁니다. 그 뒤 Catalog 포인터가 바뀌어도 내려받은 JSON은 **조회한 시점의 참조를 확인하는 기록**입니다. 파일명이 같다고 다른 시점의 current라고 계속 부르지 않습니다. 콘솔의 ETag도 보편적인 파일 내용 해시라고 가정하지 않습니다.

JSON이 한 줄로 길게 보여 읽기 어렵다면 제공된 읽기 전용 도구로 같은 필드를 확인해도 됩니다. `내가_받은_파일.metadata.json` 부분은 **자신이 방금 내려받은 파일명**으로 바꿉니다. 다운로드 위치가 다르면 폴더 경로도 바꿉니다. 원본 파일을 수정하거나 서비스에 접속하는 도구는 아닙니다.

```bash
METADATA_FILE="$HOME/Downloads/내가_받은_파일.metadata.json"
python3 labs/session03/follow-along/inspect_metadata.py "$METADATA_FILE"
```

`current-snapshot-id`와 `matching-snapshot-id`가 같고 그 항목의 `manifest-list`가 출력되는지 봅니다. [도구 코드](inspect_metadata.py)는 원본 JSON에서 같은 ID의 항목을 고르는 짧은 프로그램입니다. 파일을 못 찾으면 다운로드 폴더와 파일명을 먼저 확인합니다.

**멈춤 기준:** Catalog 주소의 파일명과 다운로드 원본이 같고, current-snapshot-id에서 같은 snapshot의 manifest-list로 이어지면 계속합니다.

## 11. 같은 환불의 `_file`과 MinIO Parquet 연결하기

[SQL 따라하기4절](README.md#4-helper-안의-bronze-sql도-직접-실행하기)의 CASE_ID 검증이 포함된 Spark SQL 블록을 다시 실행합니다. 이것은 원천 PostgreSQL용 SQL이 아닙니다.

같은 환불의 c/u/d 행에서 `parquet_file`을 읽습니다. `s3://warehouse/` 뒤가 warehouse 버킷 안의 객체 key입니다. 예를 들어 key가 `bronze/cdc_events/data/...parquet`이면 MinIO에서 **warehouse → bronze/cdc_events/data/**로 이동해 **그 파일명**을 찾습니다. ObjectInfo의 Name·Size를 보고 경로가 같은지 확인합니다. 파일이나 버킷을 삭제하지 않습니다.

다음 도구로 같은 키에 연결된 현재 파일의 행 수·크기도 읽습니다.

```bash
./labs/session02/lab.sh "$CASE_ID" files --summary --evidence
```

파일 전체 `record_count`에는 다른 이벤트도 포함될 수 있습니다. 같은 환불의 이력 수3과 파일 전체 행 수는 다르며, 여러 이벤트가 같은 파일에 들어갈 수 있습니다.

테이블 전체 파일의 표본을 직접 보고 싶다면:

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "
SELECT file_path, record_count, file_size_in_bytes
FROM lake.bronze.cdc_events.files
ORDER BY file_path LIMIT 3"
```

이 LIMIT3은 전체 테이블의 파일 샘플이며 **해당 환불의 파일3개가 아닙니다.** 표본에 내 경로가 없다는 이유만으로 누락이라 판단하지 않습니다.

snapshot 이력도 별도로 확인합니다.

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "
SELECT snapshot_id, committed_at, operation
FROM lake.bronze.cdc_events.snapshots
ORDER BY committed_at DESC LIMIT 3"
```

이 테이블의 쓰기 이력과 Catalog 조회 당시 JSON의 snapshot을 구분해 봅니다. 조회 시점이 다르면 새 상태가 추가됐을 수 있습니다. 시간상 최신 번호만으로 모든 상황의 current를 정하지 않습니다.

## 12. 실제 Parquet 내용까지 확인하기

```bash
./labs/session02/lab.sh "$CASE_ID" parquet --summary --evidence
```

이 명령은 `_file`로 찾은 실제 객체 bytes를 읽고 Parquet 행을 디코드해 SQL 결과의 payload·topic·partition·offset과 대조합니다. 화면에서 볼 값은 다음과 같습니다.

- `passed=true`, `sql_file_records_match=true`.
- `matching_record_count=3`: 이번 환불의 c/u/d 이력 수.
- 파일별 `magic_start=PAR1`, `magic_end=PAR1`, `byte_count`, `decoded_record_count`.

`.files` 조회는 파일 메타데이터이며, 원본 bytes를 읽은 이 검증과 구분합니다. 이 도구는 현재 공통 append Bronze의 선택 파일 검사입니다. 모든 Iceberg DeleteFile이나 모든 장애의 exactly-once를 검증하는 도구는 아닙니다.

**멈춤 기준:** 실제 파일 대조가 통과했고 같은 키의 c/u/d3행을 확인했다면 지연 측정으로 넘어갑니다.

## 13. 별도의 새 키로 첫 조회 지연 측정하기

앞의 `CASE_ID`는 보존합니다. 원천의 환불은 삭제돼0건이어도 **Bronze에는 c/u/d3행이 남아** 측정 도구의 “과거 이력 없음” 조건에 맞지 않습니다. 새 변수로 별도 주문·결제만 준비합니다.

```bash
LATENCY_CASE_ID=s01-s03-lat-$(date +%m%d%H%M%S)
printf '지연 CASE_ID=%s\n환불 ID=%s-r1\n' "$LATENCY_CASE_ID" "$LATENCY_CASE_ID"
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$LATENCY_CASE_ID" \
  < labs/session03/follow-along/source-prepare.sql
./labs/session02/lab.sh "$LATENCY_CASE_ID" wait --event payment --evidence
./labs/session03/inspect.sh "$LATENCY_CASE_ID"
```

`bronze_visible=true`, 원천의 `source_refund_rows=[]`, `case_bronze.record_count=0`을 확인합니다. 아직 환불을 별도로 입력하지 않습니다.

이제 다음을 **한 번만** 실행합니다.

```bash
./labs/session03/measure-latency.sh "$LATENCY_CASE_ID" \
  --insert-refund --timeout 180 --poll-interval 1 --run-label rehearsal
```

`rehearsal`은 개인 실행·사전 검증을 구분하는 표시입니다. 결과를 미리 채운다는 뜻이 아니며 실제로 실행합니다. 이 프로그램이 사전 조건을 확인하고 환불20000을 한 번 INSERT·명시적 commit한 뒤, 같은 키가 Bronze에 보이는지 반복 조회합니다. 별도의 `refund`나 `refund-insert.sql`을 선행하거나 덧실행하지 않습니다.

| 값 | 뜻 |
|---|---|
| t0 | INSERT 호출 직전 |
| t1 | 클라이언트가 commit 응답을 받은 직후. DB 내부 실제 commit 시각은 아님 |
| t2 | 같은 refund_id·payment_id·op=c·금액20000을 만족한 첫 SELECT의 반환 직후 |
| request_to_observation_seconds | t2−t0, 입력 호출부터 첫 조회 확인까지 |
| commit_ack_to_observation_seconds | t2−t1, commit 응답 뒤 첫 조회 확인까지 |

한 runner 프로세스의 같은 monotonic 시계로 측정합니다. 두 구간에는 **폴링 간격·REFRESH·쿼리·응답 시간**이 포함됩니다. 순수 Iceberg commit 지연이나 SLA가 아닙니다. UTC는 기록 시점 표시이며 Kafka·Flink의 다른 시각 컬럼을 서로 빼서 별도 지연을 만들지 않습니다.

볼 값은 `status=passed`, `outcome=observed`, `commit_outcome=acknowledged`와 `matched_record`의 자기 키·금액20000·op=c입니다. 지연 초와 offset은 개인 실행마다 다릅니다. 이 명령이 끝난 다음 다른 화면으로 넘어갑니다.

timeout이나 오류가 나면 `phase`, `status`, `outcome`, `commit_outcome`과 현재 상태를 남깁니다. 응답이 불명확해도 DB에서는 이미 commit됐을 수 있습니다. 같은 INSERT를 즉시 재시도하거나 원천 행·파일을 지우지 않습니다.

## 14. 같은 새 키를 다시 대조하고 마무리하기

측정이 끝난 뒤 원천과 Kafka·Bronze를 다시 확인합니다.

```bash
./scripts/lab.sh exec -T source-db psql -X -v ON_ERROR_STOP=1 \
  -U source -d app -v case_id="$LATENCY_CASE_ID" \
  < labs/session03/follow-along/source-check.sql
./labs/session02/lab.sh "$LATENCY_CASE_ID" kafka --summary --evidence
./labs/session02/lab.sh "$LATENCY_CASE_ID" bronze --summary --evidence
```

원천 현재 환불1건·20000, Kafka c, Bronze c1행과 같은 topic·partition·offset을 대조합니다. Kafka UI에서도 **새 LATENCY_CASE_ID-r1**으로 검색하고 Enter → Refresh합니다. 앞의 c/u/d 완료 키와 혼동하지 않습니다.

Kafka 소비 위치·Flink checkpoint·Iceberg commit·같은 키 SELECT는 각기 다른 경계입니다. 이번에는 같은 새 키의 행과 금액까지 직접 확인했습니다. 자신의 두 CASE_ID, 결과와 질문 하나를 남깁니다.

모든 확인을 마치고 **개인 환경을 종료하려는 경우에만** 다음을 실행합니다. 아래 명령은 안내이며 결과 조회를 위해 실행 상태를 유지할 때는 아직 실행하지 않습니다.

```bash
./scripts/stop.sh
```

savepoint를 남겨 중지하는 경로입니다. 일반 종료나 오류 해결에 `down -v`, 볼륨 삭제, Kafka offset 초기화를 사용하지 않습니다.
