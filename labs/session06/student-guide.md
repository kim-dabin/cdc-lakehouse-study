# 6회차 학생 가이드: 바뀐 것만 고치고도 전체 계산과 같은 답으로

5회차에서는 Silver 현재 상태 위에 공통 fact·dim(Silver core)과 Gold 정산 마트를 dbt `table` 모델로 만들고 데이터 테스트 15개로 검증했습니다. 매번 전체를 다시 읽어 테이블을 새로 쓰는 방식은 단순하고 복구가 쉽지만, 데이터가 커지면 바뀐 몇 건 때문에 전체를 다시 집계하는 부담이 커집니다.

이번 회차는 두 부분으로 이어집니다. 앞부분에서는 5회차 끝에 설계로만 봤던 **dbt 모델 계약**을 모델 다섯 개에 적용해 합의 없는 열 이름·타입 변경을 빌드 전에 막고, **OpenMetadata 컬럼 리니지**로 금액·날짜 열이 어느 마트까지 흘러가는지 확인합니다. 뒷부분에서는 그 열 계보 위에 **행 단위 증분 범위**를 얹어, 고정 입력 여섯 시나리오(신규·수정·환불 철회·과거 주문일 정정·중간 실패 후 같은 범위 재시도)에서 전체 재계산과 일치하는 Gold 증분 갱신을 확인하고 실제 운영 판단 기준까지 연결합니다.

수업 중에는 미리 준비된 데모 테이블을 Lakehouse Explorer와 OpenMetadata 화면으로 열어 보며 SQL과 결과를 예측하고 내 도메인의 키 전파 경로를 정리합니다. 집에서는 [6절](#6-집에서-따라-하기) 안내에 따라 개인 Docker 환경에서 같은 실험을 실행할 수 있습니다. 새로 작성해서 제출할 파일은 없으며 과제 형식과 기한은 별도 공지를 따릅니다.

이 가이드에서 쓰는 표시는 네 가지입니다.

- 실측: 공통 환경(Spark 3.5.6, Iceberg 1.10.0, dbt-core 1.12.5, dbt-spark 1.9.3, OpenMetadata 2.0.5)에서 실행해 확인한 값이며 날짜를 함께 적습니다. 개인 환경에서는 스냅샷 번호나 생성 시각이 다를 수 있습니다.
- 코드: 저장소 파일에 적힌 내용입니다.
- 문서: 해당 버전 공식 문서에 적힌 동작입니다.
- 설계: 수업·코드에서 직접 실행하지 않은 운영 기준, 확장 제안, 개인 프로젝트 적용 기준입니다.

## 공통 파이프라인과 6회차 실험판 비교

기존 공통 테이블(`lake.silver.*`, `lake.gold.*`)을 건드리지 않기 위해 6회차 실습은 `lake.lesson06` 네임스페이스 아래에 실행 ID(`--run-id`)마다 별도 테이블 7개를 만듭니다(코드: [`lab.py`](lab.py)).

| 구분 | 공통 파이프라인 (5회차 기준) | 6회차 격리 실험판 (`labs/session06`) |
|---|---|---|
| 입력 현재 상태 | `lake.silver.orders`·`payments`·`refunds` (`pipeline.py` MERGE) | `lake.lesson06.<prefix>_orders`·`payments`·`refunds` (테이블별 일괄 MERGE, COW 명시) |
| Silver core | dbt `table` 모델 3개 (`dim_order`, `fct_payment`, `fct_refund`) | 임시 뷰 3개 ([`sql/01-core.sql`](sql/01-core.sql), 같은 필터·날짜 규칙) |
| Gold 마트 | dbt `table` 모델 2개 (`mart_order_settlement`, `mart_daily_settlement`) | Iceberg 테이블 2개 (`..._gold_order`, `..._gold_daily`, COW 명시, `mart_built_at` 제외) |
| 정답 대조 기준 | 데이터 테스트 15개 + `verify.py` 기대값 | 매 단계 전체 재계산 뷰([`sql/02-full.sql`](sql/02-full.sql))와 양방향 `EXCEPT` 0건 비교 + 고정 기대값 |
| 상태·복구 기록 | 없음 (매번 전체 교체) | `..._progress`(마지막 성공 스냅샷) + `..._plan`(쓰기 전 저장한 영향 키·날짜 계획) |

실험판 Gold 두 테이블에서 생성 시각 열(`mart_built_at`)을 뺀 이유는 업무 열(`order_id`, `order_date`, `order_count`, `paid`, `refunded`, `net_paid`) 전체를 기준 뷰와 그대로 `EXCEPT` 비교하기 위해서입니다(코드).

## 증분 도입 판단과 다섯 가지 완료 조건

실무에서는 처음부터 증분 모델을 만들지 않고 세 가지 질문으로 전환 필요성을 먼저 판단합니다(설계 — 직접 측정하기 전에는 속도·비용 절감 수치를 주장하지 않습니다).

1. **전체 재계산 시간 대 목표 주기**: 5회차에서 가정한 목표 갱신 주기(예: 30분) 안에 전체 교체가 여유 있게 끝난다면 단순한 `table` 교체가 운영과 복구 면에서 유리합니다.
2. **주기당 변경 비율**: 한 주기에 실제로 바뀌는 주문 키와 날짜 비율이 전체 데이터에 비해 충분히 작아야 증분 처리의 이점이 생깁니다.
3. **영향 범위 계산 비용**: 변경 로그 조회, 하위→상위 키 조인 전파, `MERGE` 파일 재작성 비용을 합쳐도 전체 재계산보다 작은지 실제 환경에서 측정해 판단합니다.

증분으로 전환하기로 했다면 [`labs/README.md`](../README.md)의 아래 다섯 조건에서 모두 전체 재계산과 같은 답이 나와야 하며, 운영에서도 오늘 실험의 `verify`(양방향 `EXCEPT` 0건 확인)처럼 일·주 단위로 전체 재계산과 대조하는 **정기 대조(reconciliation)** 절차를 함께 둡니다(설계).

| 순서 | 물을 것 | 6회차 실험의 답 | 놓치면 생기는 일 | 확인 단계 |
|---|---|---|---|---|
| ① 변경 원천·진행 위치 | 어디서 변경을 읽고 언제 진행 위치를 올리나요? | MERGE로 유지하는 현재 상태 테이블에서 읽고, 두 Gold 갱신이 모두 끝난 뒤에만 `progress` 저장 | 전체 교체 테이블에서 읽으면 전 행이 변경으로 잡힘. 먼저 저장하면 실패 구간이 유실됨 | 전체, `s6` |
| ② 영향 키와 재계산 | 하위 변경이 상위 마트 키로 어떻게 전파되나요? | `orders` ∪ `payments` ∪ (`refunds` → `payments` 조인). 선택된 주문은 결제·환불 전체를 다시 합산 | 차액만 더하고 빼면 상태 변경·철회·재시도 때 금액이 틀어짐 | `s1`~`s3` |
| ③ 삭제 반영 | 삭제·철회된 행이 Gold에서 빠지거나 지워지나요? | 삭제 거르기 전 변경 뷰에서 키를 잡고, `affected_keys` 기준 `LEFT JOIN`으로 `NOT present`면 `DELETE` | 철회된 환불이나 삭제된 주문이 Gold에 그대로 남음 | `s4` |
| ④ 과거 기간 정정 | 집계 기준 열(날짜 등)이 바뀌면 어디를 고치나요? | 이전 날짜와 새 날짜를 모두 영향 날짜에 넣고, 주문이 0건이 된 날짜는 `DELETE` | 옛 날짜에 유령 합계가 남아 전체 합계가 부풂 | `s5`, `s6` |
| ⑤ 중간 실패·재시도 | 두 마트 사이에서 멈춘 뒤 재시도해도 일치하나요? | Gold를 고치기 전에 `range_id`별 영향 키·날짜를 `plan`에 저장하고 같은 범위 재시도 때 재사용 | 이미 바뀐 `gold_order`만 보고 날짜를 다시 짜서 옛 날짜 정리를 놓침 | `s6` |

## 1. 거버넌스: 모델 계약과 컬럼 리니지

### 왜 모델 계약이 필요한가요

5회차에서 다룬 데이터 테스트(`unique`, `not_null`, `relationships`, `settlement_balance`)는 **테이블을 만든 뒤에** 데이터 내용을 검사합니다. 만약 누군가 여러 마트가 함께 읽는 Silver core 모델(`fct_payment` 등)의 열 이름을 바꾸거나 숫자 열을 문자열로 바꾸면, 이미 바뀐 스키마로 테이블이 교체된 뒤에야 하류 모델이나 증분 MERGE가 깨집니다.

이를 막기 위해 저장소의 Silver core 3개([`_silver__models.yml`](../../dbt/models/silver/_silver__models.yml))와 Gold 2개([`_gold__models.yml`](../../dbt/models/gold/_gold__models.yml)) 모델 모두에 `contract: {enforced: true}`와 모든 열의 `data_type`·`description`을 적용했습니다(코드, 공통 환경 실측 2026-11-01: `dbt build` 20단계 `PASS=20`, Gold 결과 동일). 실무에서는 누군가 SQL만 고쳐 스키마를 바꾸려 할 때 YAML 계약을 함께 고쳐 하류와 합의하지 않으면 **코드 리뷰·빌드 단계에서 계약 실패로 걸러지는 보호망** 역할을 합니다(설계). [dbt model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts)

### 계약 위반 시연과 Spark의 제약 한계

[`governance/contract-failure-demo.sh`](../../governance/contract-failure-demo.sh)는 원본 `dbt/` 폴더를 건드리지 않고 `.runtime/contract-demo` 복사본에서 `fct_payment.amount`의 `data_type`만 `bigint`에서 `string`으로 바꾼 뒤 `dbt run --select fct_payment`를 실행합니다(코드).

```text
This model has an enforced contract that failed.
amount | BIGINT_TYPE | STRING_TYPE | data type mismatch
```

SQL 쿼리가 내놓는 `BIGINT_TYPE`과 YAML에 적힌 `STRING_TYPE`이 다르므로 dbt가 테이블을 교체하기 전에 실패하고, 기존 `lake.silver.fct_payment` 테이블은 바뀌지 않습니다(공통 환경 실측 2026-11-01, [`docs/governance.md`](../../docs/governance.md)).

함께 알아 둘 한계가 있습니다. 공통 환경에 설치된 dbt-spark 1.9.3 코드를 확인하면 Spark 어댑터의 `CONSTRAINT_SUPPORT`는 `not_null`을 포함한 모든 제약이 `NOT_ENFORCED`이고, `persist_constraints`도 `delta` 형식에서만 동작합니다(코드: [impl.py](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/adapters/spark/impl.py), [adapters.sql](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/adapters.sql)). 실제로 계약을 켜고 빌드한 컴파일 DDL에도 `NOT NULL` 같은 테이블 제약은 붙지 않습니다(공통 환경 실측 2026-11-01). 따라서 **열 이름·타입은 모델 계약으로 막고, 키 중복·결측·정산 차액 같은 데이터 내용은 데이터 테스트로 검증**합니다.

### OpenMetadata 연동과 컬럼 리니지

[`scripts/governance.sh`](../../scripts/governance.sh)와 [`governance/openmetadata/om_api.py`](../../governance/openmetadata/om_api.py)는 선택 프로필(`governance`)로 띄운 OpenMetadata 2.0.5에 Spark Thrift 테이블 메타데이터, dbt `manifest.json`·`run_results.json`, 그리고 5개 모델의 데이터 계약을 등록합니다(코드, [`docs/governance.md`](../../docs/governance.md)).

공통 환경 실측(2026-11-01, OpenMetadata 2.0.5):

- 테이블 메타데이터 수집 처리 18건, dbt 수집 처리 24건, 오류 0건.
- 데이터 계약 5개(`dim_order`, `fct_payment`, `fct_refund`, `mart_order_settlement`, `mart_daily_settlement`) 검증 모두 `Success` (열 스키마 및 테이블 설명 규칙 통과).
- `dbt docs generate`가 만든 `catalog.json`은 Iceberg 열 정보를 읽지 못해 노드 0개로 비어 있으므로 사용하지 않고, Spark 메타데이터와 YAML `data_type`을 함께 사용합니다. 또한 OpenMetadata 계약의 quality 항목에 dbt 테스트를 직접 묶으면 별도 파이프라인 서비스(Airflow) 실행을 시도해 `Aborted`가 되므로, 테스트 결과는 dbt 수집을 통해 각 테이블의 Data Quality 탭에 표시합니다. [OpenMetadata dbt 연동](https://docs.open-metadata.org/v2.0.x/connectors/database/dbt), [Hive 연결](https://docs.open-metadata.org/v2.0.x/connectors/database/hive), [데이터 계약 명세](https://docs.open-metadata.org/latest/how-to-guides/data-contracts/spec)

컴파일된 SQL에서 추출된 주요 컬럼 리니지입니다(공통 환경 실측 2026-11-01):

| 상류 테이블·열 | 하류 테이블·열 | 의미 |
|---|---|---|
| `silver.orders.ordered_at` | `silver.dim_order.order_date`, `ordered_at` → 두 Gold 마트의 `order_date` | 주문 시각 정정이 두 마트의 날짜 기준을 바꿈 |
| `silver.fct_payment.status` | `silver.fct_refund.payment_status` | 연결 결제의 성공 여부가 환불 집계 조건에 쓰임 |
| `silver.fct_payment.amount` | `gold.mart_order_settlement.paid`, `net_paid` → `gold.mart_daily_settlement.paid`, `net_paid` | 결제 금액이 주문·일별 두 마트의 결제액과 순결제액을 바꿈 |
| `silver.fct_refund.amount` | `gold.mart_order_settlement.refunded`, `net_paid` → `gold.mart_daily_settlement.refunded`, `net_paid` | 환불 금액이 주문·일별 두 마트의 환불액과 순결제액을 바꿈 |

(`mart_daily_settlement.order_count`는 `count(*)`로 계산되어 원본 열이 없습니다.)

## 2. 변경을 어디서, 어떻게 읽나요

### 왜 Silver core가 아니라 Silver 현재 상태에서 읽나요

저장소의 Silver core 세 모델(`dim_order`, `fct_payment`, `fct_refund`)은 dbt `table` 모델입니다([`dbt_project.yml`](../../dbt/dbt_project.yml)). 실행할 때마다 `create or replace table ... as select`로 테이블 전체를 다시 쓰므로 이전 스냅샷과 새 스냅샷을 비교해도 업무상 바뀐 행만 골라낼 수 없고, `where not is_deleted`가 이미 적용돼 있어 삭제(철회) 사건도 빠져 있습니다([Iceberg RTAS](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#replace-table-as-select)). 반면 앞단의 Silver 현재 상태 테이블은 LSN·오프셋 조건 MERGE로 바뀐 키만 갱신하고 `is_deleted` 표시를 남기므로, 6회차 실험기(`lab.py`)도 현재 상태 테이블에서 스냅샷 구간의 변경을 읽습니다(코드).

### 스냅샷 구간 고정과 `create_changelog_view`

실행기는 `progress` 테이블에서 각 입력 테이블의 마지막 성공 스냅샷(`start`)을 읽고, `{table}.refs WHERE name='main'`에서 최신 스냅샷(`end`)을 읽은 뒤 모든 입력 조회를 `VERSION AS OF {end}`로 고정합니다(코드: `lab.py` 155~175행). Iceberg의 기본 Spark 증분 읽기는 `append` 스냅샷만 지원하므로([Incremental read](https://iceberg.apache.org/docs/1.10.0/spark-queries/#incremental-read)), MERGE가 만드는 `overwrite` 스냅샷 구간에서는 `create_changelog_view` 프로시저를 호출합니다(코드: `lab.py` 179~193행).

```sql
CALL lake.system.create_changelog_view(
  table => 'lesson06.s06_demo_s5_orders',
  options => map('end-snapshot-id', '5247711452044697834', 'start-snapshot-id', '2118278423232506279'),
  changelog_view => 's06_..._changes_orders',
  identifier_columns => array('order_id'),
  compute_updates => true
)
```

- `start-snapshot-id`는 **제외**하고 `end-snapshot-id`는 **포함**합니다. 첫 실행(`start`가 `NULL`)일 때는 `end-snapshot-id`만 넘깁니다.
- Copy-on-Write(`write.merge.mode='copy-on-write'`) 테이블에서 파일 재작성 때 값 변화 없이 새 파일로 복사된 행(carry-over row)은 기본 동작으로 제거됩니다([create_changelog_view](https://iceberg.apache.org/docs/1.10.0/spark-procedures/#create_changelog_view)).
- `identifier_columns`와 `compute_updates => true`를 주면 같은 키의 변경 전후 행에 `_change_type`이 `'UPDATE_BEFORE'`와 `'UPDATE_AFTER'`로 붙습니다.
- 기본 카탈로그가 `lake`(뷰 미지원 JDBC 카탈로그)인 상태에서 임시 뷰를 지우려고 `DROP VIEW IF EXISTS <뷰이름>`을 실행하면 `JDBC catalog is initialized without view support` 오류가 발생하므로(공통 환경 실측 2026-11-02), `DROP VIEW` 없이 `create_changelog_view`가 임시 뷰를 바로 덮어쓰도록 둡니다(코드).

### COW 파일 재작성과 carry-over 직접 확인

실험기(`lab.py` 312~326행 `upsert`)는 단계마다 테이블별로 `REPARTITION(1)`을 준 `MERGE`를 한 번씩 실행합니다. 공통 환경 실측(2026-11-02, `s06-demo-s5`)에서 확인된 파일·메타데이터 변화는 다음과 같습니다.

1. **`s1` 초기 적재**: `orders`의 `o1`·`o2` 두 행이 Parquet 파일 하나(`record_count = 2`)에 들어갑니다(`operation = 'append'`, `added-data-files = 1`).
2. **`s2` 신규 추가**: `o3` 삽입 `MERGE`는 기존 파일을 건드리지 않는 `append` 스냅샷(`added-data-files = 1`, `deleted-data-files = null`)이며 1행짜리 파일이 하나 추가됩니다(`carryover_keys = []`).
3. **`s5` `o1` 주문일 정정**: `o1`이 들어 있던 2행 파일이 Copy-on-Write로 다시 써지면서 `overwrite` 스냅샷(`added-data-files = 1`, `deleted-data-files = 1`)이 생기고 현재 데이터 파일은 1행 파일(`o3`)과 새로 써진 2행 파일(`o1`·`o2`) 두 개가 됩니다.
4. **원본 `.changes` 대 `create_changelog_view` 비교**: 원본 메타데이터 테이블 `orders.changes`의 `s5` 커밋(`_change_ordinal = 2`)에는 실제 변경된 `o1`의 `DELETE`·`INSERT`(LSN `100` → `500`) 2행뿐 아니라, 값 변화 없이 새 파일로 그대로 복사된 `o2`의 `DELETE`·`INSERT`(같은 스냅샷, 같은 주문 시각 `2026-11-02 01:00:00`, `source_lsn = 100` 그대로) 2행까지 **총 4행**(`raw_rows = 4`, `raw_keys = ['o1', 'o2']`)이 나타납니다. 반면 `create_changelog_view` 결과는 carry-over인 `o2`를 제거하고 `o1` **2행**(`changelog_rows = 2`, `changelog_keys = ['o1']`, `carryover_keys = ['o2']`)만 남깁니다.

엔진 버전과 관련된 실제 함정도 확인했습니다. 공통 환경(Spark 3.5.6, Iceberg 1.10.0)에서 `.changes` 테이블에 `_commit_snapshot_id IN (서브쿼리)` 조건을 걸거나 `_change_ordinal` 없이 `_commit_snapshot_id`만 선택하면 `ClassCastException`이 발생했고, `_change_ordinal`과 `_commit_snapshot_id`를 함께 선택한 뒤 스냅샷 ID로 걸러 내면 정상 동작했습니다(공통 환경 실측 2026-11-02, 코드: `lab.py` 217~221행). 문서 설명만 믿지 않고 작은 고정 입력으로 먼저 동작을 검증해야 하는 이유입니다. (`VERSION AS OF` 교차 검증 쿼리는 [더 알아보기](#더-알아보기) 절에서 다룹니다.)

## 3. 영향 범위: 키 전파와 과거 날짜 이동

### 영향 주문 키(`affected_keys`)와 전체 재집계

Gold 주문 마트의 기준 키는 `order_id`입니다. `orders`와 `payments` 변경에는 `order_id`가 바로 있지만 `refunds`에는 `payment_id`만 있으므로 현재 시점 결제 뷰(`input_payments`)와 조인해 `order_id`로 전파합니다(코드: `lab.py` 253~258행).

```sql
CREATE OR REPLACE TEMP VIEW {affected_keys} AS
SELECT order_id FROM {changes_orders}
UNION SELECT order_id FROM {changes_payments}
UNION SELECT p.order_id
      FROM {changes_refunds} r
      JOIN {input_payments} p ON r.payment_id = p.payment_id
```

- 환불이 철회(`is_deleted = true`)되어 `fct_refund`에서 빠지더라도, 삭제를 거르기 전인 `changes_refunds`에서 변경을 읽고 결제가 보존된 `input_payments`와 조인하므로 `order_id`가 누락되지 않습니다.
- 영향 키가 정해지면 차액(delta)만 가감하지 않고, 그 주문의 현재 유효 결제·환불 전체를 5회차와 같은 식([`sql/01-core.sql`](sql/01-core.sql), [`sql/02-full.sql`](sql/02-full.sql))으로 다시 합산합니다. 차액만 더하고 빼면 결제 상태 변경·환불 철회·재시도가 겹칠 때 부호 분기가 복잡해지고 한 번만 어긋나도 누적 금액이 틀어지지만, 주문 단위 전체 재합산은 같은 범위를 다시 실행해도 결과가 흔들리지 않습니다. (대용량 환경에서는 `affected_keys`에 속한 주문·결제·환불만 먼저 걸러 같은 합산 식을 적용합니다 — 설계.)

### 과거 날짜 정정과 영향 날짜(`affected_dates`)

주문 시각(`ordered_at`)이 정정되어 서울 기준 주문일(`order_date`)이 `2026-11-01`에서 `2026-10-28`로 옮겨 가면(`s5`), 새 날짜뿐 아니라 주문이 빠져나간 옛 날짜도 다시 집계해야 합니다. 영향 날짜는 세 곳의 합집합으로 구합니다(코드: `lab.py` 260~266행).

```sql
SELECT g.order_date FROM {gold_order} g JOIN {affected_keys} k ON g.order_id = k.order_id
UNION SELECT n.order_date FROM {full_order} n JOIN {affected_keys} k ON n.order_id = k.order_id
UNION SELECT to_date(from_utc_timestamp(ordered_at, 'Asia/Seoul')) order_date
      FROM {changes_orders} WHERE _change_type IN ('UPDATE_BEFORE', 'DELETE')
```

## 4. Gold MERGE SQL과 진행 위치·계획·운영 기준

### 코드 읽기: 삭제와 빈 날짜까지 정리하는 MERGE

[`sql/03-order-merge.sql`](sql/03-order-merge.sql) 전체입니다. `affected_keys`를 왼쪽에 두고 `LEFT JOIN`해 주문이 삭제되어 `full_order`에서 사라졌을 때도(`NOT s.present`) 기존 Gold 행을 `DELETE`합니다([Iceberg MERGE INTO](https://iceberg.apache.org/docs/1.10.0/spark-writes/#merge-into)).

```sql
-- 영향 키가 사라졌어도 source에 남겨 Gold에서 지워요.
MERGE INTO {gold_order} t
USING (
  SELECT k.order_id, n.order_date, n.paid, n.refunded, n.net_paid,
         n.order_id IS NOT NULL present
  FROM {affected_keys} k LEFT JOIN {full_order} n ON k.order_id=n.order_id
) s ON t.order_id=s.order_id
WHEN MATCHED AND NOT s.present THEN DELETE
WHEN MATCHED THEN UPDATE SET
  t.order_date=s.order_date, t.paid=s.paid,
  t.refunded=s.refunded, t.net_paid=s.net_paid
WHEN NOT MATCHED AND s.present THEN INSERT (order_id,order_date,paid,refunded,net_paid)
VALUES (s.order_id,s.order_date,s.paid,s.refunded,s.net_paid)
```

[`sql/04-daily-merge.sql`](sql/04-daily-merge.sql) 전체입니다. `affected_dates`에 갱신된 `gold_order`를 `LEFT JOIN`하고 `count(o.order_id) = 0`인 옛 날짜 행을 `DELETE`해 전체 재계산(`GROUP BY order_date`)과 맞춥니다.

```sql
-- 영향 날짜만 다시 묶어요. 주문이 0건인 날짜는 지워요.
MERGE INTO {gold_daily} t
USING (
  SELECT d.order_date, count(o.order_id) order_count,
         sum(o.paid) paid, sum(o.refunded) refunded, sum(o.net_paid) net_paid
  FROM {affected_dates} d LEFT JOIN {gold_order} o ON d.order_date <=> o.order_date
  GROUP BY d.order_date
) s ON t.order_date <=> s.order_date
WHEN MATCHED AND s.order_count=0 THEN DELETE
WHEN MATCHED THEN UPDATE SET
  t.order_count=s.order_count, t.paid=s.paid,
  t.refunded=s.refunded, t.net_paid=s.net_paid
WHEN NOT MATCHED AND s.order_count>0 THEN INSERT (order_date,order_count,paid,refunded,net_paid)
VALUES (s.order_date,s.order_count,s.paid,s.refunded,s.net_paid)
```

### `plan`·`progress` 저장 순서와 실무 운영 기준

Iceberg 커밋은 한 테이블 안에서만 원자적이며 여러 테이블을 하나의 트랜잭션으로 묶지 않습니다([Iceberg Reliability](https://iceberg.apache.org/docs/1.10.0/reliability/)). 실행기는 ①입력 스냅샷 범위(`ranges`) 해시로 `range_id` 생성 ②`gold_order` 쓰기 **전에** `plan` 테이블에 `(range_id, ranges_json, keys_json, dates_json)` 저장(같은 `range_id` 재시도 시 `reused: true`로 재사용) ③`gold_order`·`gold_daily` 순서로 MERGE ④두 MERGE가 **모두 성공한 뒤에만** `progress` 갱신 순서를 지킵니다(코드: `lab.py` 239~297행). 실제 운영으로 옮길 때는 다음 사항을 함께 설계합니다.

- **중단된 실행 감지(코드·설계)**: `plan`에는 새 구간 행이 저장돼 있는데 `progress`의 `snapshot_id`가 이전 값 그대로라면 두 마트 갱신 도중 실패한 상태입니다. 8회차에서 다룰 Airflow 작업 재시도가 오늘 `s6`의 "같은 범위 다시 실행"과 같은 문제를 풉니다(설계).
- **`progress`와 스냅샷 만료(문서·설계)**: `progress`에 기록된 마지막 성공 `snapshot_id`가 유지 관리(`expire_snapshots`)로 만료되면 그 스냅샷부터는 time travel이나 changelog 읽기를 이어 갈 수 없습니다([Iceberg 1.10.0 Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/): 만료된 스냅샷은 time travel에 사용할 수 없음). 따라서 스냅샷 보존 기간을 최대 장애 복구 기간보다 길게 두거나, 만료 시 전체 재계산으로 복구하는 비상 경로를 함께 마련합니다(설계 — 7회차 유지 관리로 연결).
- **재시도 전에 새 변경이 들어오는 경우(설계·미실측)**: 실패와 재시도 사이에 입력 테이블에 새 스냅샷이 추가되는 조건과 방어 설계는 [더 알아보기](#더-알아보기) 절에서 다룹니다.

### dbt `incremental` 모델과의 세부 비교

설치된 dbt-spark 1.9.3은 Iceberg에서 `append`, `merge`, `insert_overwrite`, `microbatch` 전략을 지원합니다([incremental strategy](https://docs.getdbt.com/docs/build/incremental-strategy), [Spark configs](https://docs.getdbt.com/reference/resource-configs/spark-configs), [strategies.sql](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/materializations/incremental/strategies.sql)).

| 비교 항목 | `dbt-spark 1.9.3` 기본 `incremental` | `labs/session06` (`lab.py` + SQL) | 내 도메인 선택 기준 (설계) |
|---|---|---|---|
| 매칭 행 수정·신규 삽입 | `incremental_strategy='merge'`와 `unique_key`로 자동 생성 | `03-order-merge.sql`, `04-daily-merge.sql`에 직접 작성 | 추가·단순 수정만 있으면 dbt `merge`가 간결함 |
| 삭제된 키·0건 날짜 정리 | 기본 `merge` 매크로에 `WHEN MATCHED ... DELETE` 분기 없음 (`insert_overwrite`도 빈 파티션 삭제는 별도) | `NOT s.present` 및 `s.order_count = 0`일 때 명시적 `DELETE` | 물리 삭제·철회·날짜 이동으로 빈 그룹이 생기면 커스텀 SQL·매크로 필요 |
| 다중 입력 스냅샷 진행 위치 | 단일 대상 테이블의 시간·키 조건 필터가 기본 | 세 입력 테이블의 성공 `snapshot_id`를 `progress`에 함께 저장 | 여러 원천의 스냅샷 구간을 정확히 묶어야 하면 별도 상태 테이블 필요 |
| 마트 간 실패 시 계획 보존 | 모델마다 독립 실행되어 재시도 시 하위 모델을 다시 읽음 | 쓰기 전 `plan`에 키·날짜를 고정해 같은 범위 재시도 시 재사용 | 상위·하위 마트를 연달아 부분 갱신할 때 중간 실패 보호 필요 |

## 5. 여섯 시나리오(`s1`~`s6`)와 실패·재시도 함정

### 단계별 입력과 실측 결과 요약

세션 시간대는 `UTC`로 고정되어 있고 모든 주문 시각이 `01:00` 또는 `03:00`(UTC)이라 서울 시각(`10:00`, `12:00`)으로 바꿔도 날짜는 같습니다(코드). 공통 환경 실측(2026-11-02) 정상 모드 결과입니다.

| 단계 | 입력 변화 | 감지된 변경 키 (`carryover` 확인) | 영향 키(`keys`) / 날짜(`dates`) | 갱신 뒤 `gold_daily` (`[날짜, 건수, 결제, 환불, 순결제]`) | 전체 대조 (`EXCEPT`) |
|---|---|---|---|---|---|
| `s1` | `o1`(11-01)·`p1`(100,000)·`r1`(20,000), `o2`(11-02, 결제 없음) | orders `o1,o2`(2행 한 파일) / payments `p1` / refunds `r1` | `['o1','o2']` / `['2026-11-01','2026-11-02']` | `[11-01, 1, 100000, 20000, 80000]`, `[11-02, 1, 0, 0, 0]` | 0 / 0 일치 |
| `s2` | `o3`(11-02)·`p3`(50,000) 추가 | orders `o3`(`append` 1행 파일) / payments `p3` (`carryover: []`) | `['o3']` / `['2026-11-02']` | `[11-01, 1, 100000, 20000, 80000]`, `[11-02, 2, 50000, 0, 50000]` | 0 / 0 일치 |
| `s3` | `r1` 금액 `20,000 → 30,000` | refunds `r1` | `['o1']` / `['2026-11-01']` | `[11-01, 1, 100000, 30000, 70000]`, `[11-02, 2, 50000, 0, 50000]` | 0 / 0 일치 |
| `s4` | `r1` 철회 (`is_deleted = true`) | refunds `r1` | `['o1']` / `['2026-11-01']` | `[11-01, 1, 100000, 0, 100000]`, `[11-02, 2, 50000, 0, 50000]` | 0 / 0 일치 |
| `s5` | `o1` 주문일 `11-01 → 10-28` | orders `o1` (COW 재작성: raw 4행 중 `o2` carry-over 제외) | `['o1']` / `['2026-10-28','2026-11-01']` | `[10-28, 1, 100000, 0, 100000]`, `[11-02, 2, 50000, 0, 50000]` (`11-01` 삭제됨) | 0 / 0 일치 |
| `s6` | `o1` 주문일 `10-28 → 10-25` + 주문 마트 뒤 강제 실패·재시도 | orders `o1` (COW 재작성: raw 4행 중 `o2` carry-over 제외) | `['o1']` / `['2026-10-25','2026-10-28']` (`reused: true`) | `[10-25, 1, 100000, 0, 100000]`, `[11-02, 2, 50000, 0, 50000]` (`10-28` 삭제됨) | 0 / 0 일치 |

### `s6` 실패·재시도 함정 비교: 정상 모드 vs `--naive-dates`

`s6`는 `gold_order` MERGE 직후 예외(`InjectedFailure`)를 내어 멈춘 뒤 같은 스냅샷 범위를 바로 재시도합니다(코드: `lab.py` 360~371행). `--naive-dates` 옵션은 `plan`을 저장하지 않고 `UPDATE_BEFORE`도 읽지 않은 채 `gold_order`와 `full_order`의 날짜만 비교합니다. 공통 환경 실측(2026-11-02) 비교입니다.

| 구분 | `--naive-dates` 모드 (실측 2026-11-02) | 정상 모드 (실측 2026-11-02) |
|---|---|---|
| 1차 시도 계획 | `dates: ['2026-10-25', '2026-10-28']` (`persisted: false`) | `dates: ['2026-10-25', '2026-10-28']` (`persisted: true`) |
| 강제 실패 시점 상태 | `gold_order`의 `o1`은 `10-25`로 커밋됨. `gold_daily`에는 `10-28` 잔류. `progress` 불변 | 같음 |
| 재시도 때 계획 | `gold_order`와 `full_order` 모두 `10-25`라 **`dates: ['2026-10-25']`만 계산됨** | 저장된 계획을 재사용(`reused: true`)해 **`dates: ['2026-10-25', '2026-10-28']` 유지** |
| 재시도 뒤 `gold_order` | 전체 계산과 일치 (`0 / 0`) | 전체 계산과 일치 (`0 / 0`) |
| 재시도 뒤 `gold_daily` | **`2026-10-28` 행 잔류 (`actual_minus_full = 1`, 불일치)** | `2026-10-28` 행 삭제 완료, 전체 계산과 일치 (`0 / 0`) |

## 6. 집에서 따라 하기

### 먼저 확인할 것

명령은 **개인 컴퓨터의 저장소 루트**에서 실행합니다. 최신 코드를 받은 뒤 기본 컨테이너 스택(Spark Thrift Server, Iceberg 카탈로그 DB, MinIO, `runner`)이 실행 중인지 확인합니다([개인 공통 실행 안내](../../docs/self-study.md), [환경 운영 안내](../../docs/operations.md)).

```bash
git pull --ff-only
./scripts/lab.sh ps
```

`--run-id`는 반드시 `s06-`으로 시작하고 영문 소문자·숫자·밑줄(`_`)·하이픈(`-`)만 써서 최대 40자로 적습니다(코드: `lab.py` 97행). 테이블 이름에서는 하이픈이 밑줄로 바뀌므로 `s06-a-1`과 `s06-a_1`은 같은 테이블 이름(`s06_a_1_*`)이 됩니다.

### ① `demo.sh`로 데모 6세트 준비하고 Explorer로 살펴보기

[`demo.sh`](demo.sh)는 고정 `run-id` 6개(`s06-demo-s2`, `s06-demo-s3`, `s06-demo-s4`, `s06-demo-s5`, `s06-demo-s6`, `s06-demo-naive`)를 한 번에 실행합니다. 이 고정 `run-id` 이름들로 테이블이 만들어지므로 **Lakehouse Explorer의 `6회차 · ...` 예제 12개가 SQL 수정 없이 그대로 동작합니다**(코드: [`demo.sh`](demo.sh), [`tools/explorer/README.md`](../../tools/explorer/README.md)).

```bash
# 처음 실행할 때 (기존 데모 테이블이 있으면 멈추고 안내함)
./labs/session06/demo.sh

# 지정 데모 테이블 6세트만 지우고 다시 만들며 JSONL 로그까지 남길 때
./labs/session06/demo.sh --fresh --log .runtime/s06-demo.jsonl
```

이어서 조회 전용 화면인 Lakehouse Explorer를 실행하고 브라우저에서 `http://localhost:28090`을 엽니다.

```bash
# Explorer 기동 (Spark Thrift·MinIO는 재시작하지 않음)
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --build --no-deps explorer
```

- **SQL** 탭 왼쪽 예제 목록 아래쪽에서 `6회차 · s2 주문별 Gold`부터 `6회차 · s6 naive와 전체 재계산 차이`까지 12개 예제를 눌러 `실행`(`Ctrl/Cmd+Enter`)합니다. (조회 전용이므로 `SELECT`, `WITH`, `SHOW`, `DESCRIBE` 등만 허용되고 `CALL`, `MERGE`, `CREATE`, `DROP` 등은 거부됩니다.)
- `6회차 · s5 현재 데이터 파일` 결과의 `s3://warehouse/...` 경로를 누르면 **파일** 탭으로 이동해 Parquet 스키마·행 그룹 통계·앞 50행을 바로 볼 수 있고, **테이블** 탭에서 `lesson06`을 고르면 최근 스냅샷과 메타데이터 이력을 볼 수 있습니다.
- 실습 데이터를 남긴 채 Explorer만 중지하려면 아래 명령을 실행합니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml rm -sf explorer
```

### ② `lab.sh`로 직접 단계별 실행·비교·정리하기

자신만의 새 `RUN_ID`로 전체 연속 실행(`all`), 단계별 실행(`prepare` → `scenario` → `verify` → `inspect`), `naive` 모드 비교, 테이블 정리(`clean`)를 차례로 해 볼 수 있습니다.

```bash
# 1) 정상 모드 한 번에 실행 (s1~s6 모두 match: true, passed: true 확인)
RUN_ID="s06-all-$(date +%Y%m%d-%H%M%S)"
./labs/session06/lab.sh all --run-id "$RUN_ID"

# 2) 단계별로 끊어 실행하고 상태·changelog 살펴보기 (s1부터 순서대로 실행)
RUN_STEP="s06-step-$(date +%Y%m%d-%H%M%S)"
./labs/session06/lab.sh prepare --run-id "$RUN_STEP"
./labs/session06/lab.sh scenario --name s1 --run-id "$RUN_STEP"
./labs/session06/lab.sh scenario --name s2 --run-id "$RUN_STEP"
./labs/session06/lab.sh scenario --name s3 --run-id "$RUN_STEP"
./labs/session06/lab.sh scenario --name s4 --run-id "$RUN_STEP"
./labs/session06/lab.sh scenario --name s5 --run-id "$RUN_STEP"
./labs/session06/lab.sh scenario --name s6 --run-id "$RUN_STEP"
./labs/session06/lab.sh verify --run-id "$RUN_STEP"
./labs/session06/lab.sh inspect --run-id "$RUN_STEP"

# 3) --naive-dates 실패·재시도 불일치 재현 (s6에서 actual_minus_full: 1 확인)
RUN_NAIVE="s06-naive-$(date +%Y%m%d-%H%M%S)"
./labs/session06/lab.sh all --naive-dates --run-id "$RUN_NAIVE"
./labs/session06/lab.sh verify --run-id "$RUN_NAIVE"   # 불일치로 AssertionError 발생 확인

# 4) 실습이 끝난 개별 run-id의 지정 테이블 7개만 정리할 때
./labs/session06/lab.sh clean --run-id "$RUN_STEP"
```

### ③ 거버넌스 계약 위반 시연 따라 하기 (선택)

```bash
./governance/contract-failure-demo.sh
```

`This model has an enforced contract that failed`와 `amount | BIGINT_TYPE | STRING_TYPE | data type mismatch`가 출력되고 원본 `dbt/` 폴더와 기존 `lake.silver.fct_payment` 테이블은 바뀌지 않습니다.

### ④ OpenMetadata 켜고 리니지·데이터 계약 확인하기 (선택)

OpenMetadata는 선택 실행 스택입니다([`docs/governance.md`](../../docs/governance.md)). 메모리는 OM 서버·검색·DB를 합쳐 약 **3 GB**를 더 쓰며(공통 환경 실측 2026-11-01: 서버 1.3 GiB, Elasticsearch 1.4 GiB, DB 0.2 GiB), 수집 이미지(`openmetadata/ingestion:2.0.5`)는 **7.7 GB**이므로 실행 전에 메모리와 디스크 여유를 먼저 확인합니다. 또한 Spark Thrift가 단일 세션(`singleSession=true`)을 공유하므로 수업 중에는 수집을 돌리지 않고, 집에서 개인 환경으로 실습할 때 실행합니다.

```bash
# 1. 기본 스택이 떠 있는 상태에서 OM을 켠다 (처음에는 이미지 내려받기에 몇 분 걸린다)
./scripts/lab.sh -f compose.yaml -f compose.governance.yaml --profile governance up -d om-server

# 2. dbt build → 테이블 메타데이터 → dbt 수집 → 데이터 계약 → 리니지 확인
./scripts/governance.sh            # 마지막 dbt 결과를 그대로 쓰려면 --no-build

# 3. 브라우저: http://localhost:28585  (admin@open-metadata.org / admin, 로컬 실습용 기본값)
```

원격 컴퓨터에서 실행 중이면 `ssh -N -L 28585:127.0.0.1:28585 <host>`로 터널을 연 뒤 접속합니다(`admin` / `admin`은 로컬 실습 기본값이므로 외부에 포트를 열지 않습니다).

브라우저에서 로그인한 뒤 다음 순서로 확인합니다.

1. **테이블 열기**: 상단 검색창에서 `mart_order_settlement`를 선택하거나 `http://localhost:28585/table/cdc_lakehouse.default.gold.mart_order_settlement/lineage` 주소로 바로 이동합니다(서비스 `cdc_lakehouse` 아래 `cdc_lakehouse.default.gold.mart_order_settlement`로 표시됩니다).
2. **컬럼 리니지 확인 (`Lineage` 탭)**: **Lineage** 탭을 연 뒤 각 테이블 상자 안의 **열(Columns) 목록을 펼쳐야** `silver.fct_payment.amount` → `paid`, `net_paid` 같은 열 단위 연결선이 보입니다.
3. **데이터 계약과 테스트 결과 확인**: 같은 테이블 화면의 **Contract**(데이터 계약) 탭에서 dbt 모델 계약으로 등록된 스키마·설명 검증 결과(`Success`)를 보고, **Data Quality**(Profiler & Data Quality) 탭에서 dbt 데이터 테스트 통과 결과를 확인합니다.

실습을 마친 뒤 OpenMetadata 컨테이너를 중지하려면 아래 명령을 실행합니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.governance.yaml --profile governance stop om-server om-search om-db
```

### 명령별 목적과 출력 읽는 법

| 명령 | 목적 | 출력 JSON(`step`)에서 확인할 곳 |
|---|---|---|
| `prepare` | 격리 테이블 7개(`orders`, `payments`, `refunds`, `gold_order`, `gold_daily`, `progress`, `plan`) 생성 | `tables`에 생성된 실제 테이블 이름 |
| `scenario --name s1..s6` | 고정 입력 반영 → 증분 갱신(`s6`는 실패·재시도 포함) → 전체 계산·기대값 검증 | `carryover`(`raw_rows`, `changelog_rows`, `carryover_keys`), `crosscheck`(`match`), `plan`(`keys`, `dates`, `reused`), `verify`(`0/0`), `expected`, `scenario_result` |
| `incremental` | 현재 `progress` 이후 스냅샷 범위를 읽어 Gold 증분 갱신 (`--fail-after order`로 중간 중단 실험 가능) | `ranges`, `keys`, `dates`, 또는 `injected_failure` |
| `verify` | 현재 `gold_order`·`gold_daily`와 전체 재계산 뷰의 양방향 `EXCEPT` 건수 확인 | `actual_minus_full`, `full_minus_actual`이 모두 `0`이고 `match: true`인지 |
| `inspect` | 7개 테이블의 최근 스냅샷 5개·행 최대 5개와 최근 계획 구간의 changelog 행 조회 | `inspect`(`snapshots`, `rows`), `inspect_changelog`(`_change_type` 포함 행) |
| `all` | 새 `run-id`로 `prepare`부터 `s1`~`s6`까지 연속 실행 | 여섯 단계의 `scenario_result` |
| `clean` | 해당 `run-id`의 지정 테이블 7개만 `DROP TABLE IF EXISTS ... PURGE` 실행 | `tables`에 정리된 7개 테이블 목록 |

### 멈췄을 때 오류 확인하기

| 증상·오류 메시지 | 원인과 확인 방법 |
|---|---|
| `이미 있는 데모 run-id: ... 새로 준비하려면 demo.sh --fresh를 쓰세요.` | `demo.sh`의 고정 데모 테이블이 이미 존재합니다. 데모 6세트만 다시 만들려면 `./labs/session06/demo.sh --fresh`를 실행합니다. |
| `ValueError: run-id: s06-로 시작하고 소문자/숫자/_/-만 사용하세요(최대 40자).` | `RUN_ID` 형식이나 길이를 확인합니다. |
| `ValueError: 이 run-id의 테이블이 있어요. prepare/all은 새 run-id로 시작하세요.` | 이미 생성한 `RUN_ID`로 `prepare`·`all`을 다시 실행한 경우입니다. 새 `RUN_ID`를 만듭니다. |
| `ValueError: <테이블> 입력이 s<N>의 상태와 달라요.` | 앞 단계를 건너뛰었거나 뒤 단계까지 진행한 `RUN_ID`에서 이전 단계를 실행한 경우입니다. `s1`부터 순서대로 진행합니다. |
| `AssertionError: 이전 단계부터 전체 계산과 달라요. 먼저 incremental로 복구하세요.` | `--fail-after order`로 중간 중단된 상태입니다. 같은 `RUN_ID`로 `lab.sh incremental`을 실행해 복구합니다. |
| `verify`에서 `AssertionError: 전체 계산과 달라요.` | `--naive-dates`로 `s6`까지 실행해 `2026-10-28` 행이 남은 테이블이거나 중간 실패 후 미복구 상태입니다. |
| Spark Thrift 연결 오류 또는 Explorer 테이블 미존재 오류 | `./scripts/lab.sh ps`로 기동 상태를 확인하고, Explorer 6회차 예제 실행 전에는 `demo.sh`를 먼저 실행했는지 확인합니다. |

## 7. 내 프로젝트로 옮기기

3~5회차에 설계한 내 도메인의 원천·Silver·Gold 모델 위에 오늘 다룬 설계·운영 질문을 채워 봅니다. 근거가 아직 없는 칸은 억지로 메우지 말고 원천에서 더 확인해야 할 정보를 적습니다.

| 항목 | 내 도메인으로 적을 것 |
|---|---|
| ① 도입 판단·계약·변경 원천 | 전체 재계산 시간 대 목표 주기 비교, `contract: {enforced: true}`를 걸 공통 모델, 변경을 읽을 현재 상태 테이블 |
| ② 키 전파 그래프 | 어떤 하위 테이블의 변경이 어떤 조인 키를 거쳐 Gold 마트의 기준 키까지 올라가는지 (삭제·철회 시 키 확인 위치 포함) |
| ③ 이전·새 값이 둘 다 필요한 열 | 주문일·예약일·카테고리·상태처럼 값이 바뀌었을 때 이전 그룹과 새 그룹 집계를 모두 갱신해야 하는 열 |
| ④ 쓰기·복구·스냅샷 보존 | 사라진 키·0건 그룹을 지우는 MERGE 조건, 쓰기 전 `plan`·완료 후 `progress` 순서, `progress` 스냅샷이 만료되지 않게 둘 보존 기간과 비상 전체 재계산 경로 |
| ⑤ 정기 전체 재계산 대조 | 기준 계산(full) 쿼리와 양방향 `EXCEPT` 0건을 확인할 다섯 시나리오 및 운영 중 정기 대조 주기 |

과제 형식과 기한은 별도 공지를 따릅니다.

## 더 알아보기

### Time Travel 교차 검증으로 변경 키 대조하기 (실측·코드·설계)

실험기(`lab.py` 195~232행 `crosscheck`)는 `create_changelog_view` 프로시저가 내놓은 변경 키만 믿지 않고, `start` 시점과 `end` 시점 스냅샷을 `VERSION AS OF`로 불러 직접 `FULL OUTER JOIN`한 뒤 null-safe 비교(`<=>`)로 실제 값이 달라진 키(`time_travel_keys`)를 뽑아 매 단계 대조합니다(코드).

```sql
WITH b AS (SELECT * FROM {target} VERSION AS OF {start}),
     a AS (SELECT * FROM {target} VERSION AS OF {end})
SELECT coalesce(b.order_id, a.order_id) changed_key
FROM b FULL OUTER JOIN a ON b.order_id = a.order_id
WHERE b.order_id IS NULL OR a.order_id IS NULL OR NOT (b.col <=> a.col ...)
```

- **공통 환경 실측(2026-11-02)**: `s2`(`append`)에서는 두 방식 모두 `['o3']`만 나왔고, `s5`·`s6`처럼 파일이 Copy-on-Write로 재작성되어 `o2` carry-over가 생긴 구간에서도 두 방식 모두 `o2`를 제외하고 `['o1']`만 내놓아 데모 6세트 전 단계에서 불일치 0건으로 일치했습니다.
- **운영에서의 활용(설계)**: 대용량 테이블에서 매 주기 전체 스냅샷 두 개를 조인하는 것은 비용이 크므로, 상시 운영 경로보다는 Spark·Iceberg 엔진 버전을 올리거나 고정 입력 회귀 테스트를 실행할 때의 검증 안전장치로 활용합니다.

### 실패 뒤 재시도 전에 새 변경이 들어오면 (설계·미실측)

`s6`에서 실측한 복구는 **1차 실패 직후 입력 테이블에 새 커밋이 들어오기 전에** 같은 `(start, end)` 스냅샷 구간을 바로 재시도해 저장된 `plan`을 그대로 재사용(`reused: true`)한 결과입니다(실측·코드).

만약 실제 운영 중에 1차 시도가 `gold_order`만 갱신하고 멈춘 사이 상류 Silver MERGE가 한 번 더 돌아 입력 테이블의 최신 스냅샷이 `end2`로 이동하면, `(start, end2)` 구간으로 바뀌어 새로운 `range_id`가 만들어지고 이전 `plan`은 재사용되지 않습니다(코드: `lab.py` 239~274행).

- **코드에 넣어 둔 방어선과 미실측 한계(설계·미실측)**: `plan`을 재사용하지 못하더라도 넓어진 `(start, end2)` 구간의 `changes_orders`에서 `UPDATE_BEFORE`와 `DELETE` 행의 날짜를 읽어 이전 날짜를 놓치지 않도록 작성해 두었습니다. 다만 실패와 재시도 사이에 같은 키가 연속으로 수정되거나 삭제되는 복합 시나리오는 이번 실습에서 아직 실측하지 않았습니다(설계·미실측).
- **운영 적용 전 권장 대안(설계·미실측)**: 미완료 `plan`(`progress`보다 앞선 구간 계획)이 남아 있을 때는 새 최신 스냅샷(`end2`)으로 바로 넓히지 않고, 남아 있는 미완료 `plan`의 `ranges_json` 구간을 먼저 끝까지 실행해 `progress`를 맞춘 뒤 다음 스냅샷 구간으로 넘어가는 복구 절차를 두거나 별도 고정 입력 시나리오로 검증하는 것이 안전합니다.

## 참고 문서

| 주제 | 문서 |
|---|---|
| dbt 모델 계약 | [Model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) · [Documentation](https://docs.getdbt.com/docs/build/documentation) |
| dbt 증분 모델·Spark 설정 | [Incremental models](https://docs.getdbt.com/docs/build/incremental-models) · [Incremental strategy](https://docs.getdbt.com/docs/build/incremental-strategy) · [Spark configs](https://docs.getdbt.com/reference/resource-configs/spark-configs) |
| dbt-spark 1.9.3 코드 | [PyPI](https://pypi.org/project/dbt-spark/1.9.3/) · [impl.py](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/adapters/spark/impl.py) · [adapters.sql](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/adapters.sql) · [incremental/strategies.sql](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/materializations/incremental/strategies.sql) |
| OpenMetadata 2.0.x | [dbt 연동](https://docs.open-metadata.org/v2.0.x/connectors/database/dbt) · [Hive 연결](https://docs.open-metadata.org/v2.0.x/connectors/database/hive) · [데이터 계약 명세](https://docs.open-metadata.org/latest/how-to-guides/data-contracts/spec) · [로컬 Docker 배포](https://docs.open-metadata.org/v2.0.x/quick-start/local-docker-deployment) |
| Iceberg 1.10.0 | [MERGE INTO](https://iceberg.apache.org/docs/1.10.0/spark-writes/#merge-into) · [Incremental read](https://iceberg.apache.org/docs/1.10.0/spark-queries/#incremental-read) · [Spark Procedures](https://iceberg.apache.org/docs/1.10.0/spark-procedures/#create_changelog_view) · [Inspecting tables](https://iceberg.apache.org/docs/1.10.0/spark-queries/#inspecting-tables) · [Replace table as select](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#replace-table-as-select) · [Configuration](https://iceberg.apache.org/docs/1.10.0/configuration/) · [Reliability](https://iceberg.apache.org/docs/1.10.0/reliability/) · [Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/) |
| Spark 3.5.6 | [from_utc_timestamp](https://spark.apache.org/docs/3.5.6/api/sql/index.html#from_utc_timestamp) · [Spark SQL 설정](https://spark.apache.org/docs/3.5.6/configuration.html#runtime-sql-configuration) |
