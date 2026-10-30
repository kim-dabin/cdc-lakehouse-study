# 4회차 학생 가이드 — 변경 이력과 현재 상태

이번 시간에는 데이터를 다시 수집하기보다 **수집한 변경을 어떤 현재 상태로 해석할지** 정합니다. 이미 배운 Spark SQL을 사용해 키별 최신 후보를 고르고, 오래된 변경이 현재 값을 덮지 않도록 MERGE 조건을 작성합니다. 공통 환불 예제로 규칙을 확인한 뒤 자신의 도메인 한 테이블에 적용합니다.

수업에서는 강사 화면을 보면서 SQL·결과를 예측하고 설계를 작성합니다. 개인 환경 실행은 집에서 아래 안내를 따라 진행할 수 있습니다. 수업 중 개인 컴퓨터의 설치를 모두 끝내거나 별도 제출 파일을 새로 작성하는 활동은 아닙니다.

시연 영상을 보며 직접 실행할 때는 [시연 따라 하기](follow-along/README.md)를 사용하세요. 영상과 같은 순서로 복사할 명령과 기대 결과를 모았습니다.

## Silver는 어떤 층인가요

이번 회차의 목표는 refunds의 정답 테이블을 외우는 것이 아니라, **자기 테이블에도 같은 순서로 Silver를 설계하는 방법**을 익히는 것입니다. 먼저 층마다 맡는 일을 나눕니다.

| 층 | 이 과정의 구성 | 맡는 일 | 하지 않는 일 |
|---|---|---|---|
| 앞단 통로 | Kafka 토픽(보관 기간이 정해짐) | 변경 이벤트를 잠시 보관하고 전달 | 장기 이력 보관 |
| Bronze | Flink가 쓰는 첫 Iceberg 테이블 `lake.bronze.cdc_events` | 메시지 원문에 topic·offset 같은 위치 정보만 붙여 append, 이력 보존, 재처리 출발점 | 중복 제거·최신 상태 계산 |
| Silver | Spark SQL MERGE로 만드는 테이블별 현재 상태 | JSON 펴기·타입 맞추기·중복 정리·CDC 병합·늦은 변경 거절·삭제 표시 | 부서별 지표·집계(보통 Gold에서) |
| Gold | dbt 모델(5회차) | 지표·집계·업무 규칙, 테이블 사이 조인 | 원문 해석 |

- 첫 Iceberg 테이블에서 바로 중복을 지우고 가공하면, 원문을 따로 보관하지 않는 한 앞단 보관 기간이 지난 뒤에는 원문 이력을 다시 처리하기 어렵습니다. 이 과정의 Bronze는 값이 없는 tombstone만 빼고 메시지 원문을 그대로 둡니다. 파일을 먼저 떨구는 'Landing Zone'을 따로 두는 팀에서 그 파일을 Bronze로 부르면 이름이 한 칸씩 밀려 이런 일이 생기기 쉽습니다.
- Silver에는 집계하지 않은, 검증된 레코드 단위가 남아 있어야 합니다. 조인을 Silver에서 하는 팀도 있지만, 이 과정은 테이블별 Silver를 두고 결제·환불을 묶는 일은 Gold에서 합니다.
- Silver를 '단일 진실 공급원(SSOT)'이라고 부르는 글도 있지만, Databricks 문서는 원본 충실도를 지키는 Bronze를 single source of truth로 설명합니다. 이름보다 각 층이 무엇을 보장하는지로 말합니다.

근거: [Databricks 메달리온 아키텍처](https://docs.databricks.com/aws/en/lakehouse/medallion), [Flink 적재 SQL](../../flink/ingest.sql.template)

## Silver 설계 순서

아래 순서는 오늘 수업이 refunds로 따라가는 순서이고, 여러분 테이블에도 그대로 씁니다. 단계마다 **답, 그 답의 근거, 그 답이 깨지는 조건**을 함께 남깁니다. 근거가 없으면 억지로 정하지 말고 보류 이유와 더 필요한 원천 정보를 적습니다. 오른쪽 열은 슬라이드 번호와 [설계 학습지](design-worksheet.md)의 절입니다. 학습지 0·7절은 수업 뒤에 작성합니다. 이 순서를 공통 환불 테이블의 스키마 한 장으로 그린 그림은 [학생용 PPT](slides/04-session04-student.pdf) 31장(부록)에 있습니다. 오른쪽 표의 '설계 질문 · 열 · 타입 · 어디서'를 자기 테이블에도 그대로 채워 보세요.

| 순서 | 물을 것 | 공통 환불 예제의 답 | 깨지는 조건·확인할 근거 | 자료 |
|---|---|---|---|---|
| 1 | 누가 어떤 판단에 쓰고, 얼마나 빨리 봐야 하나요? | 정산 담당자가 오늘 기준 유효 환불을 봄 | 신선도 요구가 바뀌면 9번 갱신 방식이 바뀜 | 4장(업무 질문)·27장(신선도) · 학습지 1·7 |
| 2 | Silver 한 행은 무엇인가요? 현재 상태인가요, 버전이 유효했던 기간(SCD Type 2)인가요? | 환불 한 건의 현재 상태 + 삭제 표시 | 과거 시점 상태를 조회해야 하면 SCD Type 2 검토 | 7장(본문: 현재 상태·키, 메모: SCD Type 2) · 학습지 0·1 |
| 3 | 그 한 행을 구분하는 키는? | `refund_id`(원천 PK) | FK(`payment_id`)로 묶으면 다른 환불이 합쳐짐. PK가 바뀌는 UPDATE | 7·11장 · 학습지 1 |
| 4 | 무엇으로 최신을 판단하나요? | 숫자형 `source.lsn`, 같으면 같은 파티션의 offset | 다중 원천·다중 파티션·재스냅샷·토픽 재생성. 도착 시각은 쓰지 않음 | 9·10장 · 학습지 2 |
| 5 | 변경 종류와 삭제를 어떻게 읽나요? | c/u/r은 after, d는 before. 삭제는 표시로 보존 | REPLICA IDENTITY 설정, tombstone, 물리 삭제 정책 | 11·18장 · 학습지 3 |
| 6 | 어떤 입력을 거절하나요? | PK·LSN·금액·op 위반, 같은 전달 위치의 다른 원문 → MERGE 전에 중단 | Spark 기본 CAST는 실패하면 NULL을 돌려줌 | 8·10·12장 · 학습지 4 |
| 7 | 후보를 키마다 하나로 어떻게 줄이나요? | PK별 `ROW_NUMBER`, 삭제도 후보에 포함 | 삭제를 먼저 거르면 과거 UPDATE가 살아남음. `DISTINCT`·`MAX`로 대신 못 함 | 13–15장 · 학습지 4 |
| 8 | 저장된 상태와 어떻게 합치나요? | `ON`은 PK, `WHEN MATCHED`에 버전 가드 | 버전 조건을 `ON`에 넣으면 과거 버전이 INSERT로 빠져 중복 | 16·17장 · 학습지 5 |
| 9 | 얼마나 자주, 어떤 방식으로 갱신하나요? | 수업은 단계별 MERGE, 공통 구현은 주기 MERGE | 짧은 주기의 전체 scan·COW 재작성·작은 파일. 스트리밍은 상태·복구 | 26·27장 · 6절 · 학습지 7 |
| 10 | 무엇을 보면 맞다고 하나요? | 키 한 행 / 최신 버전 / 삭제 유지 / 같은 입력 재실행 결과 유지 / 무효 입력 중단 | 업무 결과와 파일·snapshot 변화는 따로 봄 | 5·19–24장 · 학습지 6 |

## 1. 실행 환경 준비

명령은 **개인 컴퓨터의 저장소 루트**에서 실행합니다. 강사 맥미니에 SSH로 접속하거나 강사 전용 경로를 복사하지 않습니다.

처음 받는 경우:

```bash
git clone https://github.com/kim-dabin/cdc-lakehouse-study.git
cd cdc-lakehouse-study
```

이미 clone했다면 해당 폴더에서 최신 자료를 받습니다.

```bash
git pull --ff-only
```

Bash·Python·Docker Engine·Compose v2 및 Linux ARM64 기준의 환경 조건은 [개인 공통 실행 안내](../../docs/self-study.md#실행-전에)와 [운영 안내](../../docs/operations.md)를 봅니다. 최초 build에는 네트워크와 Docker 메모리·디스크 여유가 필요합니다.

처음 설치하거나 프로젝트가 중지돼 있다면 기존 환경 안내의 **현재 상태에 맞는 한 가지**만 실행합니다. 아래 세 줄을 순서대로 실행하는 절차가 아닙니다.

| 현재 상태 | 선택할 명령 |
|---|---|
| 처음 설치 | `./scripts/self-study.sh --environment first` |
| 준비된 프로젝트가 중지됨 | `./scripts/self-study.sh --environment start` |
| 서비스가 이미 실행 중이며 기존 공통 실행도 다시 확인하고 싶음 | `./scripts/self-study.sh --environment reuse` |

서비스가 이미 실행 중이고 연결이 준비돼 있다면 위 공통 실행을 반복하지 않고 바로 4회차 실험으로 갑니다. 기존 `self-study.sh`는 앞 회차의 실제 원천·Kafka·Bronze 공통 확인을 수행하는 도구입니다. 아래의 `labs/session04/lab.sh`와 검증 범위가 다릅니다. 일부 서비스만 기동돼 있다면 기동 로그를 확인하고 상태부터 정리하세요. 볼륨 삭제나 전체 재설치로 덮어두지 않습니다.

## 2. 어떤 입력과 결과를 비교하나요?

공통 원천의 관계는 `orders(order_id)` → `payments(payment_id, order_id)` → `refunds(refund_id, payment_id)`입니다. 주문과 결제 100,000원은 유지되고, 같은 환불이 20,000원으로 생성되고 30,000원으로 정정된 뒤 철회됩니다.

이번 실행기는 **Bronze 규격의 고정 입력(fixture)**을 별도 영역에 넣습니다. 새 PostgreSQL CDC를 발생시키거나 기존 Kafka·Flink·공통 Bronze·Silver·Gold를 갱신하지 않습니다. 원천 수집의 성공·실시간 지연·SLA를 이 실험으로 증명하지 않습니다.

처음 `prepare`하면 아래 세 행이 **한꺼번에** 준비됩니다. `refund-r1`·`payment-p1`과 LSN·offset은 고정 실험용 값입니다. 실제 서버에서 관측한 번호나 자신의 프로젝트 키가 아닙니다.

같은 RUN_ID로 `prepare`를 다시 실행해도 기존 행과 Silver를 초기화하지 않습니다. 처음 상태부터 다시 확인하려면 새 RUN_ID를 사용하세요.

| op | 사용할 행 | amount | 설명용 LSN | offset | 뜻 |
|---|---|---:|---:|---:|---|
| c | after | 20000 | 100 | 10 | 환불 생성 |
| u | after | 30000 | 200 | 11 | 같은 환불 정정 |
| d | before | 30000 | 300 | 12 | 환불 철회 |

### 지난 시간 테이블과 무엇이 다른가요?

1–3회차에 본 공통 테이블을 건드리지 않으려고, 이번 실험은 실습 전용 테이블에 고정 입력을 넣어 진행합니다. 키·버전·삭제 규칙과 MERGE 가드는 공통 파이프라인(`scripts/pipeline.py`)과 같습니다.

| | 지난 시간(1–3회차) | 이번 4회차 실습 |
|---|---|---|
| Bronze 테이블 | `lake.bronze.cdc_events`(실제 수집) | `lake.lesson04.<RUN_ID>_bronze`(고정 입력) |
| Bronze와 토픽 | orders·payments·refunds **세 토픽을 한 테이블**에 함께 쌓고, Silver를 만들 때 `topic`으로 걸러 읽음 | **refunds 토픽 하나만 담는 테이블(1:1)** |
| Bronze 열 | payload·topic·partition_id·kafka_offset·kafka_ts·ingested_at | 같음 |
| 데이터 | 실제 원천 변경, 실제 LSN·offset | `refund-r1`·`payment-p1`, LSN 100/200/300, offset 10/11/12 |
| Silver 테이블 | `lake.silver.refunds` | `lake.lesson04.<RUN_ID>_refunds` |
| Silver 열 | refund_id·payment_id·amount·status·is_deleted·source_lsn·kafka_offset·source_ts_ms | 앞 7개 같음 + topic·partition_id, source_ts_ms 없음 |
| 잘못된 입력 | PK가 NULL이면 빼고, LSN이 없으면 -1로 채워 진행 | MERGE 전에 중단 |

Silver에 topic·partition_id를 둔 것은 offset이 같은 토픽·파티션 안에서만 비교할 수 있는 값이라는 점을 드러내기 위해서입니다. 여러 토픽을 한 Bronze 테이블에 모으는 방식도 쓰이는 설계입니다. 그 경우 읽을 때마다 topic으로 거르고, offset을 토픽 사이에서 비교하지 않아야 합니다. 내 프로젝트에서 어느 쪽을 고를지는 [설계 학습지](design-worksheet.md) 0절에 적습니다.

`merge --stage c`, `u`, `d`는 이미 적재된 fixture 중 LSN≤100, ≤200, ≤300 범위를 차례로 선택합니다. `prepare` 이후 실제 CDC가 차례로 도착하는 시연이 아니며, 이 상한은 운영 증분 처리의 watermark가 아닙니다.

실험별 테이블은 `lake.lesson04.<RUN_ID를 변환한 이름>_bronze`와 `..._refunds`입니다. 정확한 이름은 `prepare` 출력의 `tables`에서 확인합니다. 예를 들어 `s04-myname-01`은 테이블 이름에서 `s04_myname_01`로 바뀝니다. 하이픈과 밑줄만 달리한 RUN_ID는 같은 테이블 이름이 될 수 있으므로 다른 실험의 구분으로 사용하지 마세요.

## 3. SQL에서 결정하는 것

### 키와 중복

현재 상태 한 행의 키는 `refund_id`입니다. `payment_id`만으로 묶으면 한 결제에 연결된 서로 다른 환불을 하나로 줄일 수 있습니다. 같은 금액이라는 이유로도 중복을 판단할 수 없습니다.

| 상황 | 식별 근거 | 이번 실험의 처리 |
|---|---|---|
| 같은 전달 위치의 반복 | topic·partition·offset | 고정 입력에 같은 원문을 한 번 더 넣어도 Silver 한 키 유지 |
| 과거 원천 변경의 새 전달 | 과거 source.lsn·더 큰 offset | 과거 배치 후보를 골라도 target보다 오래되면 버전 가드로 거절 |
| 다른 PK로 만들어진 유사 업무 | 도메인의 별도 업무 규칙 | PK별 최신 선택만으로 해결하지 않음 |

같은 전달 위치에 **서로 다른 원문**이 있으면 어느 쪽이 정답인지 임의로 고르지 않고 중단합니다. 동일 LSN이 항상 재전달이라는 일반화도 하지 않습니다.

### before·after와 숫자형 LSN

[01-latest.sql](sql/01-latest.sql)을 읽습니다. 생성·정정·snapshot `r`은 after, 삭제 `d`는 before에서 키·값을 고릅니다. 삭제의 after는 null이므로 after만 읽으면 삭제 키를 놓칠 수 있습니다. 공통 원천은 REPLICA IDENTITY FULL이며, fixture도 삭제 before에 amount를 포함하는 형태입니다. 다른 원천 설정에서도 전체 before가 항상 있다고 가정하지 않습니다.

```sql
CASE WHEN get_json_object(payload, '$.op') = 'd'
     THEN get_json_object(payload, '$.before')
     ELSE get_json_object(payload, '$.after') END AS row_json
```

JSON에서 꺼낸 source.lsn을 `BIGINT`로 바꿔 비교합니다. 문자열 내림차순에서는 '9' > '10'이라 '9'가 먼저 옵니다. 숫자형 LSN의 최신 순서와 달라 CAST가 필요합니다. 이 설명용 숫자와 PostgreSQL의 다른 LSN 표기법을 섞지 않습니다.

```sql
CAST(get_json_object(payload, '$.source.lsn') AS BIGINT) AS source_lsn
```

이번 규칙은 **단일 원천·고정 토픽·파티션 0**을 전제로 합니다. 다른 원천의 LSN이나 서로 다른 파티션의 offset을 전역 버전처럼 비교하면 안 됩니다. 원천 교체·재스냅샷·토픽 재생성으로 버전 범위가 달라질 때도 다시 설계해야 합니다.

### 삭제를 포함해 최신 후보 고르기

```sql
ROW_NUMBER() OVER (
  PARTITION BY refund_id
  ORDER BY source_lsn DESC, kafka_offset DESC
) AS rn
```

이 정규화·후보 SELECT 자체가 입력 오류를 모두 거절하는 것은 아닙니다. 실행기가 먼저 **LSN 상한 필터 전의 전체 fixture**를 검사하고, 통과했을 때 이 SQL을 제출합니다. NULL LSN은 상한 비교에서 빠질 수 있으므로 필터 뒤의 행만 검사해서는 누락을 놓칠 수 있습니다.

같은 키의 변경을 묶고 숫자형 LSN 우선, 같은 LSN이면 이 예제의 같은 토픽·파티션 offset으로 순서를 정합니다. `rn=1`로 후보 하나를 고릅니다. [Spark window 공식 문서](https://spark.apache.org/docs/3.5.6/sql-ref-syntax-qry-select-window.html)에서 구문을 확인할 수 있습니다.

최신 선택 **전에** `is_deleted=false`로 거르면 후보에서 마지막 삭제가 사라집니다. 처음 Silver를 만들거나 아직 UPDATE 상태라면 삭제가 반영되지 않고 과거 UPDATE가 유효하게 남습니다. 이미 삭제 버전이 반영된 대상은 버전 가드가 보호하지만, 잘못된 후보 선택 자체를 해결하는 것은 아닙니다. 삭제도 후보에 포함하고 유효 업무 조회에서 나중에 제외합니다. `DISTINCT`나 `MAX(amount)`는 이 규칙을 대신하지 않습니다.

### MERGE의 버전 조건

ROW_NUMBER는 **이번 입력 배치 안**의 후보를 하나로 줄입니다. MERGE 버전 가드는 그 후보를 **이미 저장된 Silver 상태**와 비교합니다. 입력 배치에서 1등이어도 Silver에 더 최신 버전이 있으면 갱신하면 안 됩니다.

[02-merge.sql](sql/02-merge.sql)의 `ON`은 같은 엔티티를 찾고, `WHEN MATCHED`는 더 새 버전만 갱신하도록 합니다. 입력은 먼저 PK별 한 후보로 좁힙니다. NULL PK는 정렬에서 한 묶음이 될 수 있지만 MERGE의 `NULL = NULL`은 참이 아니므로 반복 INSERT로 이어질 위험이 있습니다. 따라서 누락을 조용히 버리는 대신 MERGE 전에 중단합니다. 하나의 대상 행을 여러 원본 행이 갱신하려 하면 문제가 생길 수 있습니다. [Iceberg MERGE 공식 문서](https://iceberg.apache.org/docs/1.10.0/spark-writes/#merge-into)

```sql
ON t.refund_id = s.refund_id
WHEN MATCHED AND (
  s.source_lsn > t.source_lsn
  OR (s.source_lsn = t.source_lsn AND s.kafka_offset > t.kafka_offset)
)
THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

이 코드는 판단을 읽기 위한 발췌입니다. 그대로 실행할 완성 SQL은 [템플릿](sql/02-merge.sql)에 있습니다. 템플릿의 `{bronze}`, `{silver}`, `{incoming}`, `{upper_lsn}`은 실행기가 해당 RUN_ID에 맞춰 채웁니다. 중괄호를 남긴 채 DB 도구에 붙여넣지 않습니다.

Silver는 삭제 행과 마지막 버전을 남깁니다. 최종 `amount=30000, is_deleted=true`는 철회 직전 금액을 가진 삭제 표시 행입니다. **현재 유효한 환불 30,000원이 있다는 뜻이 아닙니다.** `WHERE NOT is_deleted`로 읽으면 0행이어야 합니다.

## 4. 개인 환경에서 실행하기

### 한 번에 확인

새 실험마다 새 RUN_ID를 만듭니다. 영문 소문자로 시작하며 소문자·숫자·하이픈·밑줄, 최대 40자를 사용합니다. 아래 자동 값도 사용할 수 있습니다.

```bash
RUN_ID="s04-$(date +%Y%m%d-%H%M%S)"
./labs/session04/lab.sh all --run-id "$RUN_ID"
```

`all`은 준비→최신 후보→c/u/d MERGE→중복·과거 UPDATE·동일 입력 재실행→최종 검증을 수행합니다. 완료한 RUN_ID에 `all`을 다시 실행하면 데이터를 초기화하지 않고 새 RUN_ID를 사용하라는 오류를 냅니다.

### 단계별로 확인

전체 실행과 별개로 **새 RUN_ID**를 만들고 다음 순서로 진행할 수 있습니다. `all`을 끝낸 테이블을 c 단계로 되돌리는 절차가 아닙니다. 버전 가드는 과거 상태로의 덮어쓰기를 거절합니다.

```bash
RUN_ID="s04-step-$(date +%Y%m%d-%H%M%S)"
./labs/session04/lab.sh prepare --run-id "$RUN_ID"
./labs/session04/lab.sh latest --run-id "$RUN_ID"
./labs/session04/lab.sh merge --stage c --run-id "$RUN_ID"
./labs/session04/lab.sh merge --stage u --run-id "$RUN_ID"
./labs/session04/lab.sh merge --stage d --run-id "$RUN_ID"
./labs/session04/lab.sh replay --run-id "$RUN_ID"
./labs/session04/lab.sh verify --run-id "$RUN_ID"
```

| 명령 | 무엇을 확인하나요? | 출력에서 볼 곳 |
|---|---|---|
| prepare | 전용 테이블과 초기 c/u/d 고정 입력 준비 | `tables`, `input_kind`, `merge_mode` |
| latest | 모든 고정 입력의 최신 후보 | `rows`: 환불 30000·삭제 true·LSN300·offset12 |
| merge --stage c | 생성 단계 현재 상태 | `stored_rows`: 20000·false·100·10 |
| merge --stage u | 정정 단계 현재 상태 | `stored_rows`: 30000·false·200·11 |
| merge --stage d | 삭제 상태 보존 | `stored_rows`: 30000·true·300·12 / `current_rows`: 빈 배열 |
| replay | 같은 삭제 전달 중복, 과거 배치의 UPDATE와 기존 삭제 비교, 동일 입력 재실행 | `delivery_duplicate`, `old_update_replay`, `same_input_rerun` |
| verify | 키 유일성·삭제·유효행·전달 중복 대조 | `passed`, `current_business_row_count`, `bronze_delivery_duplicates` |

같은 입력의 MERGE만 다시 확인하려면 완료한 RUN_ID를 그대로 사용합니다.

```bash
./labs/session04/lab.sh merge --stage all --run-id "$RUN_ID"
./labs/session04/lab.sh verify --run-id "$RUN_ID"
```

`replay`는 같은 삭제 전달 위치 offset12를 한 번 더 넣고, 과거 UPDATE LSN200을 새 offset13으로 추가합니다. 과거 UPDATE를 검사할 때는 **LSN≤200인 입력 배치**에서 최신 후보 u200/13을 골라 기존 target d300/12와 비교합니다. 전체 입력에서 다시 d300을 고르는 것과 달리, 이 단계는 입력 후보가 대상보다 오래됐을 때 MERGE 가드가 삭제 상태를 지키는지 확인합니다.

`replay` 자체를 반복하면 고정 Bronze에 재전달 행이 계속 추가됩니다. 처음부터 다시 실험하려면 새 RUN_ID를 사용하세요. 기존 테이블·파일을 지우는 초기화는 필요하지 않습니다.

## 5. 결과와 로그 읽기

검증 기준은 **①PK 한 행 ②최신 버전 ③삭제 유지 ④동일 입력 재실행의 업무 결과 유지 ⑤무효 입력의 MERGE 전 중단**입니다. 아래 상태값과 실행기 입력 검사를 함께 봅니다.

새 RUN_ID에서 `all`을 한 번 완료한 최종 기대값입니다. 기존 공통 Bronze 전체 건수나 모든 재처리 상황의 보장이 아닙니다.

| 확인 | 기대 |
|---|---|
| Silver 현재 snapshot의 보존 행 | refund-r1 한 행 |
| 저장된 amount·삭제 표시 | 30000·true |
| 보존한 최신 source.lsn·offset | 300·12 |
| 유효한 현재 환불 수 | 0 |
| 고정 Bronze 전체 | 5행 |
| 같은 전달 위치의 중복 | topic=cdc.public.refunds, partition=0, offset=12가 2행 |
| 늦게 재전달된 과거 UPDATE | source.lsn=200, 새 offset=13 |

`stored_rows`는 **현재 snapshot에서 조회되는 보존 행**입니다. 과거 snapshot·옛 파일에 남아 있는 행까지 실제 저장소 전체에서 하나라는 뜻은 아닙니다.

`stored_rows`의 순서는 `refund_id, amount, is_deleted, source_lsn, kafka_offset`입니다. 따라서 최종 `['refund-r1', 30000, true, 300, 12]`를 읽습니다. 최신 후보 `latest.rows`는 컬럼 구성이 더 길므로 [SELECT의 컬럼 순서](sql/01-latest.sql)를 함께 봅니다.

결과는 저장소의 `.runtime/session04/<RUN_ID>/report.json`에 누적됩니다.

- `tables`: 실제 실습 테이블과 임시 뷰 이름.
- `results`: 성공한 명령의 단계별 기록. 분리해 실행한 명령도 같은 RUN_ID라면 누적됩니다.
- `stored_rows`·`current_rows`: 삭제 표시를 보존한 행과 유효 업무 행의 차이.
- `old_update_replay`: `incoming_lsn=200`, `incoming_kafka_offset=13`과 `target_lsn=300`, `target_kafka_offset=12`, `guard_rejected=true`를 비교합니다. 입력 배치 안 최신과 기존 대상 최신이 다르다는 근거입니다.
- `before_storage`·`after_storage`: 현재 data 파일 경로·record_count와 최신 snapshot의 id·parent·operation.
- `verify.passed=true`: 이 고정 실험의 명시적 검증 통과. 다른 원천·스키마·모든 장애의 정확성을 뜻하지 않습니다.

오류가 나면 그 명령의 새 보고서 저장까지 도달하지 않을 수 있습니다. 보고서가 없거나 직전 성공 기록까지만 있다고 해서 오류를 추측으로 채우지 마세요. 터미널의 마지막 출력과 오류를 같이 확인합니다. 실행 로그를 남기려면 새 전체 실험에서 다음처럼 저장할 수 있습니다.

```bash
RUN_ID="s04-log-$(date +%Y%m%d-%H%M%S)"
mkdir -p .runtime/session04
set -o pipefail
./labs/session04/lab.sh all --run-id "$RUN_ID" 2>&1 | tee ".runtime/session04/${RUN_ID}.log"
```

이 로그는 보고서와 별개로 콘솔 출력을 저장한 파일입니다. 동작이 궁금하면 [실행기](lab.py)를 읽어보세요. Python은 SQL 제출 순서·검증·기록을 조정하며, JSON 변환·순위·MERGE 계산은 Spark SQL이 실행합니다.

## 6. 파일·commit·운영 비용

SQL은 대략 **Bronze 스캔→JSON 해석·타입 변환→키별 재분배·정렬→최신 후보→대상과 매칭→파일 쓰기→Iceberg commit**으로 이어집니다. 논리 단계와 물리 stage가 항상 일대일인 것은 아닙니다. 실행 계획·데이터 분포·최적화에 따라 달라지므로 성능을 판단할 때 실제 계획과 읽기·정렬·파일 비용을 봅니다. [Spark 성능 문서](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html)

이 실습은 `write.merge.mode=copy-on-write`를 명시합니다. 한 행 변경도 영향받는 data 파일을 새로 쓰는 작업이 될 수 있습니다. 기존 파일을 제자리에서 덮어쓰는 것으로 생각하지 않습니다. [Iceberg 쓰기](https://iceberg.apache.org/docs/1.10.0/spark-writes/)·[테이블 설정](https://iceberg.apache.org/docs/1.10.0/configuration/)

실제 Mac mini 검증에서는 **동일 입력을 다시 MERGE해도 결과 행은 같지만 새 overwrite snapshot과 데이터 파일이 생성됐습니다.** 따라서 업무 결과의 멱등성을 물리 no-op·snapshot 불변·쓰기 비용 0으로 해석하지 않습니다. 재시도 주기·반복 갱신·작은 파일 증가를 함께 고려합니다. 이 현상이 모든 Spark·Iceberg 버전과 모든 MERGE 계획에서 똑같다는 일반화도 하지 않습니다.

Iceberg commit은 한 테이블의 상태 변경 경계입니다. 기존 기준 구현처럼 orders→payments→refunds를 순서대로 갱신하면 중간 실패 때 일부 테이블만 갱신될 수 있습니다. 세 테이블이 한 번에 rollback되는 구성은 아닙니다. 이번 격리 실험은 refunds 한 테이블의 규칙을 확인합니다. [Iceberg 일관성과 commit](https://iceberg.apache.org/docs/1.10.0/reliability/)

기존 [scripts/pipeline.py](../../scripts/pipeline.py)는 전체 Bronze를 매번 읽는 기준 구현입니다. MERGE를 사용한다고 입력 스캔까지 증분인 것은 아닙니다. 4회차 실행기의 입력 검사와 기존 기준 코드의 PK NULL 제외·LSN -1 fallback도 동일한 정책으로 보지 않습니다.

기준 구현을 짧은 주기로 반복하면 비용이 쌓입니다. 지금처럼 Bronze에 모든 이력이 남아 있으면 입력에 모든 키의 최신 후보가 들어가고, Silver의 모든 키가 MERGE의 `ON` 조건에 걸립니다. copy-on-write는 매칭된 행이 든 파일을 다시 쓰므로, 값이 바뀌지 않아도 Silver 전체를 다시 쓸 수 있습니다. 5분마다 실행하고 매번 5분 안에 끝난다고 세면 하루 최대 288번입니다. 예전 파일은 이전 snapshot이 참조하므로 snapshot 만료 전까지 저장소에 남습니다. Bronze도 Flink checkpoint 간격이 10초라서, 변경이 조금씩 들어오면 작은 파일이 쌓입니다. 파일 정리는 7회차에서 다룹니다. [Iceberg 유지 관리](https://iceberg.apache.org/docs/1.10.0/maintenance/)

Iceberg의 Spark 증분 읽기와 스트리밍 읽기는 append snapshot만 처리합니다. append만 하는 Bronze는 이어 읽을 수 있지만, MERGE가 overwrite snapshot을 만드는 Silver는 같은 방식으로 바뀐 행을 받을 수 없습니다. 그래서 5회차 Gold는 Silver의 현재 상태를 정해진 주기에 배치로 읽도록 설계합니다. [증분 읽기](https://iceberg.apache.org/docs/1.10.0/spark-queries/#incremental-read)·[스트리밍 읽기](https://iceberg.apache.org/docs/1.10.0/spark-structured-streaming/)

| 선택 | 맞는 조건 | 함께 맡을 비용·판단 |
|---|---|---|
| 전체 재생성 | 작은 데이터, 하루 단위 신선도 | 전체 읽기·쓰기 비용과 교체 범위 |
| 주기 Spark MERGE (이번 기준 구현) | 수십 분 단위, SQL로 후보·버전 규칙을 드러내고 검증 | 매번 입력 scan·정렬·대상 매칭·COW 재작성, 주기만큼의 지연 |
| Spark Structured Streaming + 같은 MERGE | 분 단위, 같은 후보 규칙과 버전 가드를 유지 | 입력을 micro-batch로 바꾸기, 상시 작업, commit 수·파일 정리, 같은 batch 재실행에도 결과가 유지되도록 설계 |
| Flink 키별 최신 행 + Iceberg upsert | 더 짧은 지연 요구 | 버전 비교가 테이블이 아니라 job 상태로 이동, 상태 복구·TTL, v2 테이블·PK·upsert 설정, Iceberg 내부 삭제 파일 정리(업무 열 `is_deleted`와 다름) |

어느 방식이든 키·버전·삭제 규칙은 같고, 실행 방식과 복구 방법이 달라집니다. **두 스트리밍 방식은 이번 수업에서 설계로만 다루었고 실습 실행기에는 구현되어 있지 않습니다.** 어느 방식이 항상 더 빠르다고 단정하지 않습니다. 입력량·변경 비율·요구 지연·조회 패턴·운영 여력을 기준으로 선택합니다. Spark 스트리밍의 `foreachBatch` 기본 보장은 at-least-once이고, Iceberg는 스트리밍 쓰기의 trigger 간격을 최소 1분으로 시작하도록 권고합니다. Flink 상태 TTL(`table.exec.state.ttl`)의 기본값은 0으로 상태를 지우지 않습니다. Flink 방식은 기존 Silver와 맞는 상태에서 시작하거나 checkpoint·savepoint로 복구해야 합니다. 상태를 잃거나 만료되면, 또는 상태 없이 과거 입력부터 다시 쓰면 늦게 온 과거 변경이 현재 값을 덮을 수 있습니다.

참고로 Silver를 dbt incremental 모델로 만들 경우, dbt-spark 1.9.3의 기본 merge는 `WHEN MATCHED`에 조건을 붙이지 않습니다. 버전 조건을 `incremental_predicates`로 넣으면 `ON` 조건에 합쳐져 과거 버전이 `NOT MATCHED`로 빠지고 중복 INSERT가 생길 수 있습니다. 이번 버전 가드를 그대로 쓰려면 merge 매크로를 프로젝트에서 바꿔야 합니다. [dbt-spark 1.9.3 배포](https://pypi.org/project/dbt-spark/1.9.3/)·[merge 매크로 소스](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/materializations/incremental/strategies.sql#L24)

증분 범위·과거 정정·실패 후 재처리의 구체 구현은 6회차로 이어갑니다.

## 7. 멈췄을 때 확인하기

| 증상 | 먼저 볼 것 |
|---|---|
| Docker·runner 연결 실패 | 개인 Docker와 서비스 기동 상태, 선택한 first/start/reuse 상태 |
| run-id 오류 | 소문자 시작·허용 문자·40자 이내인지 |
| all에서 기존 Silver 오류 | 완료한 테이블인지. 같은 입력 확인은 `merge --stage all`, 전체 새 실험은 새 RUN_ID |
| c/u 단계의 예상값 오류 | 이미 d까지 실행한 테이블을 과거 단계로 돌리려 한 것은 아닌지 |
| 입력 계약 위반 | PK·숫자 LSN·금액·op·고정 토픽·파티션·offset. MERGE 전에 중단된 이유 |
| 동일 전달 위치에 다른 원문 | 어떤 원문이 맞는지 임의로 고르지 말고 입력 생성·전달 근거 확인 |
| 숫자·삭제 결과가 예상과 다름 | 실제 RUN_ID와 출력 테이블, before/after 선택, 최신 후보, MERGE 조건 |
| 행은 같지만 snapshot·파일 변경 | COW 반복 실행에서 관찰한 현상인지. 업무 결과와 쓰기 비용 분리 |

실행기는 LSN 상한 필터를 적용하기 전의 전체 fixture에서 잘못된 PK·LSN·금액·op·전달 정보 또는 상충 원문을 만나면 **MERGE 전에 중단**합니다. 정규화 SELECT를 실행기 없이 단독 실행한 것까지 같은 검증을 제공하는 것은 아닙니다. 잘못된 행을 조용히 건너뛰어 정상처럼 보이게 하지 않습니다. 다만 완전한 운영 데이터 품질 검증기나 모든 스키마 변화의 처리기는 아닙니다.

질문할 때는 마지막 명령, RUN_ID, 사용한 환경(first/start/reuse 또는 기존 기동), 마지막 오류, 해당 `report.json`의 마지막 단계와 로그를 함께 남겨주세요. 실제 비밀번호·회사 데이터는 보내지 않습니다. 기존 테이블·파일·볼륨을 지워 오류를 없애기보다 확인한 증거를 남깁니다.

## 8. 내 프로젝트로 옮기기

이번 주 도메인·원천 DDL·관계·변경사례 초안에서 **한 테이블**을 골라 [개인 Silver 설계 학습지](design-worksheet.md)를 작성합니다. 작성 순서는 위의 [Silver 설계 순서](#silver-설계-순서)와 같습니다. 처음 약 12분은 **한 행의 의미·키 / 버전·삭제 규칙 / 최신 후보 SQL 뼈대** 세 가지를 우선 정합니다. 남은 약 8분은 상세 타입·MERGE·기대값으로 확장합니다. 다음 여섯 항목은 완성할 방향이며 준비 상태에 따라 이어서 작성합니다.

1. 한 행의 의미와 키.
2. 최신 버전의 근거와 적용 범위.
3. 수정·삭제 정책.
4. 정규화·최신 후보 SQL.
5. MERGE 버전 조건.
6. 신규·수정·삭제·동일 입력·삭제 뒤 과거 변경의 기대값.

환경이 준비돼 있으면 실행 결과도 확인하고, 아직 구현 전이면 필요한 원천 정보와 보류 이유를 적습니다. 같은 SQL을 도메인 이름만 바꿔 복사하기보다 내 키·순서·삭제 의미가 맞는지 설명하는 것이 목표입니다. 별도 수업 중 제출이나 새 마감은 이 가이드에서 만들지 않습니다. 제출 방법은 공지를 따르며 최종 발표는 8회 스터디 이후 별도입니다.
