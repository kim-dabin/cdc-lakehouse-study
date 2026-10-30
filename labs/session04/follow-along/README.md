# 4회차 따라 하기 — 변경 이력에서 현재 상태로

시연 영상(`04-session04-demo.mp4`, 3분 45초)과 같은 순서로 내 컴퓨터에서 직접 실행해 보는 안내예요. 영상과 정리 PDF(`4회차-핵심정리.pdf`)는 스터디 공지에 함께 올라온 파일을 받아 주세요. 환불 한 건이 **20,000원 생성 → 30,000원 정정 → 철회**된 이력이 Silver의 한 행이 되는 과정을 명령 하나씩 확인해요.

시작하기 전에 몇 가지만 기억해 주세요.

- 명령은 **개인 컴퓨터의 저장소 루트에서, 같은 터미널에** 실행해요. 아래 코드 블록을 **하나씩** 복사해 붙여 넣고, 끝난 걸 확인한 다음 다음 블록으로 넘어가 주세요. 여러 블록을 한꺼번에 붙여 넣으면 앞 명령이 뒤 줄을 입력으로 읽어 버릴 수 있어요.
- 이 실습은 Bronze와 같은 형태의 **고정 입력**으로 Silver 규칙만 확인해요. PostgreSQL에 새 변경을 만들지 않고, 공통 Bronze·Silver·Gold 테이블도 건드리지 않아요.
- 아래 "기대 결과"는 2026-10-30에 실제로 실행한 출력이에요(RUN_ID `s04-20261030-225238`). 여러분 화면에서는 **RUN_ID · 테이블 이름 · 파일 경로 · snapshot ID · 시각**이 다르게 나오고, 금액·삭제 표시·LSN·offset 같은 업무 값은 같아야 해요.
- "예측" 칸에서는 결과를 보기 전에 잠깐 멈추고 답을 먼저 생각해 보세요. 정답은 접어 두었어요.

| 영상 위치(약) | 이 문서 |
|---|---|
| 00:14 | [0. 준비](#0-준비) |
| 00:25 – 00:35 | [1. RUN_ID 만들기](#1-run_id-만들기) · [2. 고정 입력 준비](#2-고정-입력-준비-prepare) |
| 00:46 – 01:04 | [예측 1](#예측-1--최신-후보는) · [3. 이력 펼치기](#3-이력-펼치기--row_number) · [4. 최신 후보](#4-merge에-넘길-최신-후보-latest) |
| 01:14 – 01:54 | [예측 2](#예측-2--생성과-정정을-차례로) · [5–7. c·u·d MERGE](#5-생성-반영-merge---stage-c) · [예측 3](#예측-3--삭제-뒤-전체와-유효) · [8. 저장된 행과 유효한 행](#8-저장된-행과-유효한-행) |
| 02:06 – 02:54 | [예측 4](#예측-4--같은-입력을-다시-넣으면) · [9. 같은 입력 재실행](#9-같은-입력-재실행-merge---stage-all) · [예측 5](#예측-5--삭제-뒤-과거-update) · [10. replay](#10-중복-전달과-과거-update-replay) · [11. verify](#11-최종-확인-verify) |
| 03:10 – 03:34 | [12. snapshot 이력](#12-snapshot-이력과-파일) · [13. 잘못된 입력](#13-잘못된-입력은-merge-전에-멈춤) · [14. 정리](#14-정리--내-테이블에-적용하기) |

## 0. 준비

저장소 루트로 이동해 위치를 확인해 주세요. `compose.yaml`, `scripts`, `labs`가 보이면 맞게 오셨어요.

```bash
pwd; ls
```

최신 실습 파일을 받아요. 처음이시면 `git clone https://github.com/kim-dabin/cdc-lakehouse-study.git` 후 그 폴더로 이동해 주세요.

```bash
git pull --ff-only
```

실행 중인 서비스를 확인해요. 목록에 `spark`, `runner`, `catalog-db`, `minio`가 있고 STATUS가 `Up`이면 바로 1절로 가시면 돼요.

```bash
./scripts/lab.sh ps
```

서비스가 꺼져 있거나 처음 설치하시는 거라면, [학생 가이드 1절](../student-guide.md#1-실행-환경-준비)에서 **지금 상태에 맞는 한 가지**(`first` / `start` / `reuse`)만 실행해 주세요. 세 명령을 차례로 다 실행하는 절차가 아니에요.

## 1. RUN_ID 만들기

실험할 때마다 새 RUN_ID를 만들어요. 출력된 값을 꼭 메모해 두세요. 터미널을 새로 열면 변수가 사라지거든요.

```bash
RUN_ID="s04-$(date +%Y%m%d-%H%M%S)"; echo "RUN_ID=$RUN_ID"
```

이어서 SQL 조회에 쓸 테이블 이름을 변수로 만들어요. 테이블 이름에는 RUN_ID의 하이픈이 밑줄로 바뀌어 들어가요.

```bash
P="${RUN_ID//-/_}"; BRONZE="lake.lesson04.${P}_bronze"; SILVER="lake.lesson04.${P}_refunds"; printf 'RUN_ID=%s\nBRONZE=%s\nSILVER=%s\n' "$RUN_ID" "$BRONZE" "$SILVER"
```

> 터미널을 새로 열었다면 메모한 값으로 `RUN_ID="s04-..."`를 먼저 다시 설정하고, 위 변수 블록도 한 번 더 실행해 주세요. `RUN_ID=`가 비어 있으면 아래 조회가 모두 실패해요.

## 2. 고정 입력 준비 (prepare)

전용 Bronze·Silver 테이블을 만들고, 생성·정정·철회 세 이력을 한꺼번에 넣어요.

```bash
./labs/session04/lab.sh prepare --run-id "$RUN_ID"
```

기대 결과:

```text
{"step": "prepare", "tables": {"bronze": "lake.lesson04.s04_20261030_225238_bronze", "silver": "lake.lesson04.s04_20261030_225238_refunds", ...}, "input_kind": "fixed Bronze-shaped fixture; not new CDC collection", "source_unchanged": true, "merge_mode": [["write.merge.mode", "copy-on-write"]]}
REPORT: .runtime/session04/s04-20261030-225238/report.json
```

- `tables`의 이름이 1절의 `BRONZE`·`SILVER`와 같은지 봐 주세요.
- `merge_mode`가 `copy-on-write`예요. 12절에서 이 설정이 왜 중요한지 다시 볼게요.
- 들어간 입력은 아래 세 행이에요. LSN·offset은 설명용 고정 값이에요.

| op | 사용할 행 | amount | LSN | offset | 뜻 |
|---|---|---:|---:|---:|---|
| c | after | 20000 | 100 | 10 | 환불 생성 |
| u | after | 30000 | 200 | 11 | 같은 환불 정정 |
| d | before | 30000 | 300 | 12 | 환불 철회 |

## 예측 1 · 최신 후보는?

세 이력 중 MERGE에 넘길 최신 후보는 c·u·d 중 무엇일까요? 그 행의 `is_deleted`는 어떨까요?

<details>
<summary>정답 보기</summary>

**d(LSN 300)이고, `is_deleted`는 true예요.** 도착 순서가 아니라 원천에서 일어난 순서(LSN)로 판단해요. 삭제도 후보에서 빼지 않아요.

</details>

## 3. 이력 펼치기 · ROW_NUMBER

세 이력을 한 줄씩 펼쳐서 `rn`이 어떻게 붙는지 볼게요. 아래 블록은 **한 번에** 복사해 붙여 넣어 주세요(큰따옴표 안의 여러 줄이 명령 하나예요).

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "
WITH e AS (
 SELECT get_json_object(payload,'$.op') op,
        CAST(get_json_object(payload,'$.source.lsn') AS BIGINT) source_lsn,
        kafka_offset,
        CASE WHEN get_json_object(payload,'$.op')='d'
             THEN get_json_object(payload,'$.before')
             ELSE get_json_object(payload,'$.after') END row_json
 FROM $BRONZE
)
SELECT op,get_json_object(row_json,'$.refund_id') refund_id,
       CAST(get_json_object(row_json,'$.amount') AS BIGINT) amount,
       source_lsn,kafka_offset,
       ROW_NUMBER() OVER (
         PARTITION BY get_json_object(row_json,'$.refund_id')
         ORDER BY source_lsn DESC,kafka_offset DESC) rn
FROM e ORDER BY source_lsn"
```

기대 결과(열 순서: op, refund_id, amount, source_lsn, kafka_offset, rn):

```text
[["c", "refund-r1", 20000, 100, 10, 3], ["u", "refund-r1", 30000, 200, 11, 2], ["d", "refund-r1", 30000, 300, 12, 1]]
```

- `rn`은 ROW_NUMBER가 같은 환불 안에서 **최신 버전부터** 붙인 번호예요. `DESC`로 줄을 세웠기 때문에 숫자가 작을수록 최신이에요.
- LSN 300인 삭제가 `rn=1`이에요.
- `$BRONZE`는 터미널이 1절에서 만든 테이블 이름으로 바꿔 줘요. SQL 편집기(DBeaver 등)에 붙여 넣으실 때는 `$BRONZE`를 실제 이름으로 바꿔 주세요.

## 4. MERGE에 넘길 최신 후보 (latest)

```bash
./labs/session04/lab.sh latest --run-id "$RUN_ID"
```

기대 결과(`rows` 열 순서: refund_id, payment_id, amount, status, is_deleted, source_lsn, topic, partition_id, kafka_offset):

```text
{"step": "latest", "upper_lsn": 9223372036854775807, "rows": [["refund-r1", "payment-p1", 30000, "success", true, 300, "cdc.public.refunds", 0, 12]], "meaning": "delete 포함해서 최신 1행 선택; 이 단계에서 삭제 행을 제외하지 않음"}
```

- 삭제를 미리 빼지 않고 후보에 그대로 넣었어요.
- 금액 30000은 삭제 직전 값이라, 지금 유효한 환불액은 아니에요.
- 이 SQL은 [01-latest.sql](../sql/01-latest.sql)에서 읽어 볼 수 있어요.

## 예측 2 · 생성과 정정을 차례로

이번엔 LSN 100까지(생성), 그다음 LSN 200까지(정정)만 나눠서 MERGE해 볼게요. 두 번 MERGE한 뒤 Silver는 몇 행이고, 금액은 얼마일까요?

<details>
<summary>정답 보기</summary>

**1행, 30000원이에요.** 같은 `refund_id` 한 행의 값이 바뀌는 거지, 행이 늘거나 금액이 더해지는 게 아니에요.

</details>

## 5. 생성 반영 (merge --stage c)

```bash
./labs/session04/lab.sh merge --stage c --run-id "$RUN_ID"
```

기대 결과(줄여서 보여 드려요):

```text
{"step": "latest", "upper_lsn": 100, "rows": [["refund-r1", "payment-p1", 20000, "success", false, 100, "cdc.public.refunds", 0, 10]], ...}
{"step": "merge", "stage": "c", "stored_rows": [["refund-r1", 20000, false, 100, 10]], "current_rows": [["refund-r1", 20000]], ... "after_storage": {..., "latest_snapshot": [[<snapshot ID>, null, "append"]]}}
```

- 처음 보는 키라서 INSERT됐어요.
- `stored_rows`는 refund_id, amount, is_deleted, source_lsn, kafka_offset 순서예요.
- 첫 쓰기라서 snapshot은 `append`예요.

## 6. 정정 반영 (merge --stage u)

```bash
./labs/session04/lab.sh merge --stage u --run-id "$RUN_ID"
```

기대 결과:

```text
{"step": "merge", "stage": "u", "stored_rows": [["refund-r1", 30000, false, 200, 11]], "current_rows": [["refund-r1", 30000]], ... "latest_snapshot": [[<새 snapshot ID>, <앞 snapshot ID>, "overwrite"]]}
```

- 같은 `refund_id` 한 행의 금액이 30000으로 바뀌었어요. 두 금액을 더한 게 아니에요.
- snapshot은 앞 snapshot을 parent로 둔 `overwrite`로 새로 생겼어요.

## 예측 3 · 삭제 뒤 전체와 유효

삭제까지 반영하면 Silver 전체를 SELECT했을 때 몇 행일까요? `WHERE NOT is_deleted`로 거른 유효 환불은 몇 행일까요?

<details>
<summary>정답 보기</summary>

**전체 1행, 유효 0행이에요.** 행을 지우는 대신 `is_deleted=true` 표시로 남기기 때문이에요.

</details>

## 7. 철회 반영 (merge --stage d)

```bash
./labs/session04/lab.sh merge --stage d --run-id "$RUN_ID"
```

기대 결과:

```text
{"step": "merge", "stage": "d", "stored_rows": [["refund-r1", 30000, true, 300, 12]], "current_rows": [], ...}
```

같은 행이 `is_deleted=true`, LSN 300으로 바뀌고, 유효 행(`current_rows`)은 비었어요.

## 8. 저장된 행과 유효한 행

저장된 행 전체를 조회해요.

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT refund_id,amount,is_deleted,source_lsn,kafka_offset FROM $SILVER"
```

```text
[["refund-r1", 30000, true, 300, 12]]
```

이번엔 유효한 환불만 조회해요.

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT refund_id,amount FROM $SILVER WHERE NOT is_deleted"
```

```text
[]
```

- 전체 1행, 유효 0행이에요. 삭제를 표시로 남겨 두었으니 행이 1개 보여도 삭제가 실패한 건 아니에요.
- 이 삭제 행과 LSN 300이 남아 있어야, 나중에 늦게 온 과거 변경을 거절할 수 있어요(10절).

## 예측 4 · 같은 입력을 다시 넣으면

같은 입력 전체를 한 번 더 MERGE하면 업무 행은 어떻게 될까요? 파일과 snapshot도 그대로일까요?

<details>
<summary>정답 보기</summary>

**업무 행은 그대로예요. 하지만 파일과 snapshot은 새로 생겨요.** copy-on-write라서 값이 같아도 매칭된 행이 든 파일을 다시 쓰거든요.

</details>

## 9. 같은 입력 재실행 (merge --stage all)

```bash
./labs/session04/lab.sh merge --stage all --run-id "$RUN_ID"
```

기대 결과(줄여서 보여 드려요):

```text
{"step": "merge", "stage": "all", "stored_rows": [["refund-r1", 30000, true, 300, 12]], "current_rows": [],
 "before_storage": {"current_files": [[".../00000-8744-...parquet", 1]], "latest_snapshot": [[442603149691404208, ...]]},
 "after_storage":  {"current_files": [[".../00000-8776-...parquet", 1]], "latest_snapshot": [[543322637156761633, 442603149691404208, "overwrite"]]}}
```

- 업무 결과는 그대로예요. 같은 PK 한 행, 삭제 유지, 유효 0행이에요.
- 그런데 `before_storage`와 `after_storage`를 비교해 보면 파일 경로와 snapshot이 바뀌었어요. 값은 같아도 새로 쓴 거예요.
- copy-on-write MERGE는 ON 조건에 매칭된 행이 든 파일을 다시 쓰기 때문이에요.

## 예측 5 · 삭제 뒤 과거 UPDATE

이번엔 같은 삭제 전달(offset 12)을 한 번 더 넣고, LSN 200의 과거 UPDATE도 새 offset 13으로 넣어 볼게요. offset 13이 가장 크니까 삭제가 풀릴까요?

<details>
<summary>정답 보기</summary>

**안 풀려요.** offset보다 LSN을 먼저 비교하는데, 200은 저장된 300보다 작아요. 더 오래된 변경은 삭제를 되살리지 못해요.

</details>

## 10. 중복 전달과 과거 UPDATE (replay)

```bash
./labs/session04/lab.sh replay --run-id "$RUN_ID"
```

출력이 여러 줄 나와요. 아래 줄들을 찾아 주세요.

```text
{"step": "delivery_duplicate", "expectation": "Bronze may contain duplicate delivery rows; Silver stays one deleted business key"}
{"step": "latest", "upper_lsn": 200, "rows": [["refund-r1", "payment-p1", 30000, "success", false, 200, "cdc.public.refunds", 0, 13]], ...}
{"step": "old_update_replay", "incoming_lsn": 200, "incoming_kafka_offset": 13, "incoming_is_deleted": false, "target_lsn": 300, "target_kafka_offset": 12, "guard_rejected": true, "stored_rows": [["refund-r1", 30000, true, 300, 12]], ...}
{"step": "same_input_rerun", "unchanged": true}
```

- LSN 200까지의 입력에서는 u200/offset13이 1등 후보가 돼요. 이 후보를 그대로 MERGE에 넘겼어요.
- 결과는 `guard_rejected=true`예요. 입력 200이 저장된 300보다 작아서 버전 가드가 거절했어요.
- ROW_NUMBER는 **이번 입력 안**의 최신만 고르고, **저장된 상태와의 비교**는 MERGE 가드가 맡아요. 가드 조건은 [02-merge.sql](../sql/02-merge.sql)의 `WHEN MATCHED AND (...)`에서 볼 수 있어요.
- `replay`를 반복하면 재전달 행이 계속 추가돼요. 처음부터 다시 보고 싶으시면 새 RUN_ID로 시작해 주세요.

## 11. 최종 확인 (verify)

```bash
./labs/session04/lab.sh verify --run-id "$RUN_ID"
```

기대 결과:

```text
{"step": "verify", "passed": true, "stored_rows": [["refund-r1", 30000, true, 300, 12]], "current_business_row_count": 0, "bronze_delivery_duplicates": [["cdc.public.refunds", 0, 12, 2]]}
```

키 한 행, 최신 버전, 삭제 유지, 유효 0행을 모두 확인했어요. Bronze에는 무엇이 남았는지도 볼게요.

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT get_json_object(payload,'$.op') op, CAST(get_json_object(payload,'$.source.lsn') AS BIGINT) source_lsn, kafka_offset FROM $BRONZE ORDER BY kafka_offset"
```

```text
[["c", 100, 10], ["u", 200, 11], ["d", 300, 12], ["d", 300, 12], ["u", 200, 13]]
```

Bronze는 5행이에요. offset 12가 두 번(같은 전달 중복), 과거 UPDATE는 offset 13으로 남았어요. Bronze에는 이력이 모두 남고, Silver에는 `refund-r1` 한 행만 남아요.

## 12. snapshot 이력과 파일

Silver 테이블에 쌓인 snapshot을 순서대로 조회해요.

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT committed_at,snapshot_id,parent_id,operation FROM $SILVER.snapshots ORDER BY committed_at"
```

기대 결과(ID와 시각은 여러분 값이 달라요):

```text
[["2026-10-30 13:52:43.230000", 7606378695136529657, null, "append"],
 ["2026-10-30 13:52:44.747000", 9203586462212840949, 7606378695136529657, "overwrite"],
 ... overwrite가 4개 더 ...
 ["2026-10-30 13:52:51.087000", 4194510780289551609, 4585795263175428740, "overwrite"]]
```

현재 data 파일과 MERGE 방식도 확인해요.

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT file_path,record_count FROM $SILVER.files"
```

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SHOW TBLPROPERTIES $SILVER ('write.merge.mode')"
```

```text
[["s3://warehouse/lesson04/<접두어>_refunds/data/00000-...-00001.parquet", 1]]
[["write.merge.mode", "copy-on-write"]]
```

- 이 문서 순서대로 실행했다면 MERGE가 7번 일어나서 snapshot이 7개예요. 첫 번째만 `append`, 나머지 6개는 `overwrite`예요.
- 현재 파일은 1개(행 1개)예요. 예전 파일은 이전 snapshot이 참조하고 있어서 snapshot을 만료하기 전까지 저장소에 남아요. 파일 정리는 7회차에서 다뤄요.
- 업무 결과가 같아도 쓰기는 일어나요. 그래서 재실행 비용은 파일과 snapshot으로 따로 살펴봐요.

| # | 어떤 MERGE | operation |
|---|---|---|
| 1 | `merge --stage c` | append |
| 2 | `merge --stage u` | overwrite |
| 3 | `merge --stage d` | overwrite |
| 4 | `merge --stage all` (같은 입력 재실행) | overwrite |
| 5 | `replay` — 같은 전달 위치 반복 뒤 MERGE | overwrite |
| 6 | `replay` — 과거 UPDATE 입력 (가드가 거절했지만 파일은 새로 씀) | overwrite |
| 7 | `replay` — 같은 입력 재실행 | overwrite |

## 13. 잘못된 입력은 MERGE 전에 멈춤

영상의 이 장면은 따로 실행한 2026-10-26 기록이에요. 실습 실행기에는 잘못된 입력을 일부러 만드는 명령이 없어서, 이 부분은 영상과 아래 기록으로 확인해 주세요.

```text
{"case": "null-pk", "rejected": true, "silver_unchanged": true}
{"case": "null-lsn", "rejected": true, "silver_unchanged": true}
{"case": "conflict", "rejected": true, "silver_unchanged": true,
 "reason": "같은 전달 위치에 서로 다른 원문이 있습니다: [('cdc.public.refunds', 0, 11)]. 임의로 최신 행을 고르지 않고 중단합니다."}
```

PK가 없거나, LSN이 비었거나, 같은 전달 위치에 다른 원문이 있으면 MERGE하지 않고 멈춰요. 실행기는 LSN 상한 필터를 걸기 **전의** 전체 입력을 먼저 검사해요. Spark 기본 설정에서는 CAST가 실패해도 오류 대신 NULL이 나오기 때문에, 이렇게 먼저 막아 두는 거예요. 검사 코드는 [lab.py](../lab.py)에서 볼 수 있어요.

## 14. 정리 · 내 테이블에 적용하기

여기까지 확인한 다섯 가지예요.

- [ ] **키 한 행** — 중복 전달과 과거 UPDATE가 들어와도 `refund-r1` 한 행만 남아요.
- [ ] **최신 버전** — 도착 순서가 아니라 LSN으로, LSN이 같으면 offset으로 골라요.
- [ ] **삭제 유지** — 삭제를 표시로 남기고, 늦게 온 과거 변경은 거절해요.
- [ ] **같은 입력 재실행** — 업무 결과는 같지만 파일과 snapshot은 새로 생겨요.
- [ ] **잘못된 입력** — MERGE 전에 멈춰요(10/26 기록).

이 순서를 여러분 테이블에도 하나씩 적용해 보세요. [설계 학습지](../design-worksheet.md) 1·2·4절부터 채워 보시고, 막히면 [학생 가이드 7절](../student-guide.md#7-멈췄을-때-확인하기)과 정리 PDF(`4회차-핵심정리.pdf`)를 참고해 주세요.

최종 결과를 한 번에 다시 확인하고 싶으시면 **새 RUN_ID**로 아래처럼 실행하시면 돼요. 이미 끝낸 RUN_ID로 `all`을 다시 실행하면 새 RUN_ID를 쓰라는 오류가 나와요.

```bash
RUN_ID="s04-$(date +%Y%m%d-%H%M%S)"; ./labs/session04/lab.sh all --run-id "$RUN_ID"
```

- `all`은 9절의 같은 입력 재실행을 따로 하지 않아서 MERGE가 6번이에요. 업무 결과는 같지만, 12절의 snapshot 7개와는 개수가 달라요. 영상과 같은 순서를 보고 싶으시면 위의 단계별 명령을 따라 주세요.
- 앞의 SQL로 새 실험을 조회하시려면 [1절의 테이블 변수 블록](#1-run_id-만들기)도 다시 실행해 주세요. 그대로 두면 이전 실험의 테이블을 읽어요.

## 막혔을 때

| 이런 일이 생기면 | 먼저 확인해 주세요 |
|---|---|
| `RUN_ID`나 `$SILVER`가 비어 있어요 | 새 터미널인지 확인하고, 메모한 RUN_ID와 1절 변수 블록을 다시 실행해 주세요. |
| 붙여 넣은 뒤 명령이 실행되지 않았어요 | 블록을 하나씩 붙여 넣었는지 봐 주세요. 여러 블록을 한꺼번에 넣으면 뒤 줄이 앞 명령의 입력으로 들어갈 수 있어요. |
| run-id 오류가 나요 | 영문 소문자로 시작하고, 소문자·숫자·하이픈·밑줄로 40자 이내인지 봐 주세요. |
| c·u 단계 결과가 기대와 달라요 | 이미 d까지 실행한 RUN_ID로 앞 단계를 다시 실행한 건 아닌지 봐 주세요. 버전 가드가 과거 상태로 덮어쓰는 걸 막아요. 새 RUN_ID로 처음부터 해 주세요. |
| SQL 편집기에서 PARSE 오류가 나요 | `$BRONZE`, `<prefix>`, `{bronze}` 같은 자리표시를 실제 테이블 이름으로 바꿨는지 봐 주세요. |
| `JDBC catalog is initialized without view support` 오류가 나요 | `prepare`를 실행했는지, 테이블 이름이 맞는지 먼저 확인해 주세요. 메시지가 안내하는 `jdbc.schema-version=V1` 설정은 하지 마세요. |
| 그 밖의 오류 | [학생 가이드 7절](../student-guide.md#7-멈췄을-때-확인하기)을 봐 주세요. 테이블·파일·볼륨을 지워서 맞추지 말고, 마지막 명령·RUN_ID·오류·`report.json`을 남겨 질문해 주세요. |
