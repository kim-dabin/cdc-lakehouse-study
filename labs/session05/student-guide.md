# 5회차 학생 가이드: 현재 상태에서 믿을 수 있는 지표로

지난 시간에는 변경 이력으로 환불 한 건의 현재 상태를 Silver로 만들었습니다. 이번 시간에는 그 현재 상태로 정산 담당자가 볼 지표를 만들어 봅니다. SQL을 쓰기 전에 숫자가 무엇을 뜻하는지 먼저 정하고 숫자가 틀렸을 때 알아챌 테스트도 함께 설계합니다.

이번 회차는 설계와 코드 읽기가 중심이라 수업 중에 새 실습을 실행하지 않고 저장소의 `dbt/` 코드와 공통 환경에서 미리 확인한 출력을 함께 읽습니다. 집에서 조회만 하는 명령을 실행해 보고 싶다면 [9절](#9-집에서-조회만-해-보기-선택)을 참고하세요. 새로 작성해서 제출할 파일은 없으며 과제 형식과 기한은 별도 공지를 따릅니다.

이 가이드에서 쓰는 표시는 세 가지입니다.

- 실측: 2026-10-31 공통 환경에서 조회만 해서 확인한 값입니다. 여러분 환경에서는 행 수·시각·snapshot 수가 다를 수 있습니다.
- 코드: 저장소 파일에 적힌 내용입니다.
- 설계: 실행으로 확인하지 않은 제안입니다.

## Gold는 어떤 층인가요

| 층 | 이 과정의 구성 | 맡는 일 |
|---|---|---|
| Bronze | Flink가 쓰는 Iceberg 테이블 | 변경 원문을 그대로 append, 재처리 출발점 |
| Silver | Spark SQL MERGE | 엔티티 한 행의 현재 상태, 삭제 표시(`is_deleted`) 보존 |
| Gold | dbt 모델 → Spark → Iceberg | 업무 지표·집계, 테이블 사이 조인 |

Gold는 Silver의 현재 상태를 읽어 지표를 만듭니다. 4회차에서 본 것처럼 MERGE로 갱신하는 Silver는 append만 하는 테이블과 달리 새로 붙은 행만 골라 이어 읽기 어렵습니다. 그래서 이번 회차의 Gold는 가장 단순한 방법인 배치로 시작합니다. 정해진 주기마다 Silver의 현재 상태 전체를 다시 읽어 테이블을 새로 만들고 공통 예제는 그 주기를 30분으로 가정합니다(설계, 7절). 바뀐 부분만 골라 반영하는 증분 방식은 6회차에서 무엇이 필요한지와 함께 다룹니다.

## Gold 설계 순서와 아홉 가지 질문

4회차의 Silver 설계 순서처럼 Gold도 답보다 질문 순서를 가져갑니다. 단계마다 답과 근거, 그 답이 깨지는 조건을 함께 적고 근거가 없으면 칸을 비워 둔 채 확인할 원천 정보를 적습니다.

| 묶음 | 물을 것 | 공통 예제의 답 | 깨지는 조건·확인할 것 |
|---|---|---|---|
| ① 쓰임·정의 | 누가 어떤 판단에, 얼마나 자주 보나요? | 정산 담당자, 주문일별 순결제액, 30분쯤 늦어도 됨 (설계 가정) | 요구 신선도가 바뀌면 갱신 방식이 바뀜 |
|  | 무엇을 더하고 빼나요? 무엇이 아닌가요? | 성공 결제 − 성공 환불(철회 제외). 회계 매출 아님 | 부서마다 '매출' 정의가 다름 |
| ② 단위·시간 | 한 행은 무엇인가요? | fct: 주문 1건, mart: 주문일 하루 | 단위가 섞이면 합계가 중복됨 |
|  | 어떤 날짜, 어느 시간대인가요? | 주문일, 서울 기준 | Spark 세션 시간대가 바뀌면 날짜가 달라짐 |
| ③ 집계 대상·조인 | 어떤 행을 세나요? | 삭제 표시 제외, `status='success'` | 상태 값이 늘면 조건 재검토 |
|  | 어떻게 묶어야 숫자가 부풀지 않나요? | 결제·환불을 주문 단위로 먼저 집계 후 조인 | Silver에 키가 중복되면 조인이 부풂 |
| ④ 테스트 | 어떻게 틀릴 수 있고 무엇으로 막나요? | unique·not_null·규칙·기댓값 | 테스트 통과는 정의한 조건만 보장 |
| ⑤ 갱신·관리 | 얼마나 자주, 어떤 방식으로 다시 만드나요? | table 전체 교체, Gold 30분 (설계) | 데이터가 커지면 증분 검토(6회차) |
|  | 누가 소유하고 무엇을 약속하나요? | 설명·계보·소유, 열 계약 (설계) | 계약은 열 모양만, 내용은 테스트 |

정산이 아닌 도메인이어도 질문은 같습니다. 영상 시청 로그라면 집계 대상은 '테스트 계정·봇 제외'가 되고 '사용자 상태 이력과 조인하면 시청 수가 복제된다'는 점이 fan-out 주의점이 됩니다. 예약 도메인이라면 날짜를 접수일로 볼지 완료일로 볼지부터 정합니다.

## 1. dbt가 하는 일

dbt는 SELECT 문과 Jinja로 작성한 모델을 실제 SQL로 컴파일해 모델 사이 의존 순서대로 실행하는 도구로, 테스트와 설명도 같은 저장소에서 함께 관리합니다.

| dbt가 하는 일 | dbt가 아닌 것 |
|---|---|
| SQL+Jinja 컴파일, 의존 순서대로 실행 | 계산 엔진이 아닙니다. 계산과 Iceberg 쓰기는 Spark Thrift Server가 합니다 |
| 데이터 테스트 실행 | 데이터를 옮기는 도구가 아닙니다 |
| 설명·계보(lineage) 관리 | 스케줄러가 아닙니다. 주기 실행은 Airflow가 맡습니다 |

대안은 Spark SQL 파일 몇 개를 스크립트로 순서대로 실행하는 방법이고 모델이 적을 때는 그것으로도 충분합니다. 모델이 늘어날수록 순서·테스트·설명을 사람이 직접 맞춰야 하는 부담이 커집니다. 반대로 dbt를 사용하면 Jinja가 한 겹 끼기 때문에 실제로 실행된 SQL은 컴파일 결과로 따로 확인해야 하고 복잡한 절차 로직은 PySpark가 더 편할 수 있습니다.

저장소 `dbt/` 폴더의 파일별 역할입니다.

| 파일 | 맡는 일 | 우리 프로젝트 (코드) |
|---|---|---|
| [`dbt_project.yml`](../../dbt/dbt_project.yml) | 프로젝트 이름·기본 설정 | 모든 모델 `materialized: table`, `file_format: iceberg` |
| [`profiles.yml`](../../dbt/profiles.yml) | 접속 정보 | Spark Thrift(`spark:10000`), schema `gold` |
| [`models/sources.yml`](../../dbt/models/sources.yml) | 원천·모델 설명과 테스트 | Silver 세 테이블, 테스트 다섯 개 |
| [`models/*.sql`](../../dbt/models/) | SELECT 하나 = 모델 하나 | `fct_order_settlement`, `mart_daily_settlement` |
| [`tests/*.sql`](../../dbt/tests/settlement_balance.sql) | 규칙을 어긴 행을 찾는 SELECT | `settlement_balance` |

모델 파일에는 CREATE 문이 없고 테이블로 만들지 뷰로 만들지는 materialization 설정이 정합니다. 최신 문서는 YAML에서 `data_tests:` 키를 쓰지만 저장소의 `tests:` 키도 이번 버전에서 그대로 인식됩니다(실측: 6 data tests).

## 2. source·ref와 dbt build

모델 SQL에는 테이블 이름 대신 함수를 씁니다.

- `{{ source('silver','payments') }}`: `sources.yml`에 선언한 원천 테이블입니다. 컴파일하면 `silver.payments`가 되고(실측: `dbt compile`), Spark 기본 카탈로그가 `lake`이므로 `lake.silver.payments`와 같은 테이블입니다.
- `{{ ref('fct_order_settlement') }}`: 다른 모델을 가리킵니다. ref로 연결해야 dbt가 실행 순서와 계보를 알 수 있습니다. 테이블 이름을 직접 쓰면 쿼리는 실행될 수 있어도 의존 관계가 끊깁니다.

공통 파이프라인([`scripts/pipeline.py`](../../scripts/pipeline.py))은 Silver를 갱신한 뒤 `dbt build`를 실행합니다. `dbt build`는 `dbt run`을 모두 끝낸 다음 `dbt test`를 따로 돌리지 않고 의존 순서대로 모델을 실행한 뒤 바로 그 모델의 테스트를 실행합니다. 상류 모델의 테스트가 실패하면 그 모델에 의존하는 하류 모델은 SKIP됩니다. 예를 들어 `fct_order_settlement`의 unique 테스트가 실패하면 `mart_daily_settlement`는 다시 만들어지지 않고 이전 테이블이 그대로 남습니다.

그런데 테스트는 fct를 만든 뒤에 실행되므로 fct는 이미 새 내용으로 바뀐 상태입니다. 결국 fct는 새 상태, mart는 이전 상태로 남아 두 테이블의 기준 시점이 어긋납니다. 남은 mart도 최신이 아니므로 실패를 알리고 원인을 고쳐 다시 빌드하는 장치가 필요하며 이 부분은 8회차에서 운영과 함께 다룹니다. [dbt build](https://docs.getdbt.com/reference/commands/build)

실측: `dbt=1.12.5`, `spark=1.9.3`, `Found 2 models, 6 data tests, 3 sources`.

## 3. 지표 정의·집계 단위·시간대

### 지표를 먼저 정의합니다

| 지표 | 정의 | 아닌 것 |
|---|---|---|
| 결제액 `paid` | 주문의 성공 결제 합, 삭제 표시 제외 | 실패·취소된 결제 |
| 환불액 `refunded` | 성공 환불 합, 삭제 표시 제외. 환불에 연결된 결제도 같은 조건 | 철회된 환불 |
| 순결제액 `net_paid` | `paid − refunded`, 주문일 기준 | 회계 매출, 환불일 기준 정산 |

[`sources.yml`](../../dbt/models/sources.yml)의 설명에는 `Order-date basis net payments, not accounting revenue.`라고 적혀 있습니다. 회계 매출이 아니라는 점까지 적어 두면 다른 부서 숫자와 다를 때 무엇이 다른지 바로 설명할 수 있습니다.

주문일 기준이라 10월 30일 주문이 10월 31일에 환불되면 그 환불은 10월 30일 숫자에서 빠집니다. 이미 본 과거 날짜의 숫자도 나중에 바뀔 수 있으며 이 문제는 6회차 소급 정정에서 다시 다룹니다.

### 한 행은 무엇인가요

- `fct_order_settlement`: 한 행이 주문 1건이고 `order_id`가 한 번만 나와야 합니다.
- `mart_daily_settlement`: 한 행이 주문일 하루이고 `order_date`가 한 번만 나와야 합니다.

단위가 정해지면 unique라는 테스트 규칙이 생깁니다. Silver에서 바로 날짜별로 모으지 않고 주문 단위를 거치는 이유는 세 가지입니다. 주문마다 `paid − refunded`가 맞는지 확인하기 쉽고 어느 날 숫자가 이상할 때 원인 주문을 되짚을 수 있으며 주문월별 집계 같은 다른 마트도 같은 fct에서 만들 수 있습니다.

### 날짜와 시간대

```sql
to_date(from_utc_timestamp(o.ordered_at, 'Asia/Seoul'))
-- 2026-10-30 15:30 (UTC) → 2026-10-31 00:30 (서울) → 2026-10-31
```

공통 환경의 Spark는 세션 시간대가 UTC입니다([`spark/start-thrift.sh`](../../spark/start-thrift.sh), 실측으로도 확인). 이 식은 `ordered_at`을 UTC 시각으로 보고 서울 시각으로 바꾼 뒤 날짜만 남깁니다. **내 프로젝트에 이 식을 옮길 때는 세션 시간대부터 확인하세요.** 세션 시간대가 이미 서울이면 같은 식이 다른 결과를 낼 수 있습니다. [from_utc_timestamp](https://spark.apache.org/docs/3.5.6/api/sql/index.html#from_utc_timestamp)

## 4. 셀 행 고르기와 fan-out

### 어떤 행을 셀까요

| 테이블 | 조건 | 이유 |
|---|---|---|
| orders | `not is_deleted` | 삭제된 주문 제외 |
| payments | `not is_deleted`, `status='success'` | 실패·취소 결제 제외 |
| refunds | `not is_deleted`, `status='success'` | 철회된 환불 제외 |
| 환불에 연결된 결제 | 결제도 `not is_deleted`, `status='success'` | 삭제됐거나 success가 아닌 결제의 환불 제외 |

4회차에서 Silver가 삭제를 지우지 않고 `is_deleted`로 남겨 둔 덕분에 Gold는 철회된 환불을 알아보고 정확히 뺄 수 있습니다. 공통 예제(결제 100,000원, 환불 30,000원 철회)의 순결제액은 100,000원이고 저장소의 [`scripts/verify.py`](../../scripts/verify.py)도 실제 원천 변경 시나리오에서 환불 삭제 뒤 `net_paid=100000`을 기댓값으로 확인합니다. 삭제 표시를 거르지 않으면 70,000원으로 틀립니다.

환불을 결제에 조인할 때는 Silver payments에 `payment_id`가 한 행씩만 있다는 4회차의 보장이 전제입니다. 같은 키가 두 행이면 환불액이 두 번 세집니다.

### 조인이 숫자를 부풀리는 경우 (fan-out)

결제 100,000원 한 건에 환불 10,000원과 20,000원이 있다고 해 보겠습니다(손 계산 예시).

| 방법 | 결과 |
|---|---|
| 결제와 환불을 먼저 조인한 뒤 합계 | 결제 행이 환불 수만큼 복제되어 2행 → `sum(결제)=200,000` ✗ |
| 결제·환불을 각각 주문 단위로 먼저 합계한 뒤 주문에 조인 | 결제 100,000, 환불 30,000, 순결제액 70,000 ✓ |

1:N 관계에서 합계를 내기 전에 조인하면 한쪽 값이 행 수만큼 복제되기 때문에 fct는 결제와 환불을 각각 먼저 집계합니다.

## 5. 코드 읽기

[`fct_order_settlement.sql`](../../dbt/models/fct_order_settlement.sql) 전체입니다.

```sql
-- Aggregate payments and refunds independently to avoid join fan-out.
with payments as (
  select order_id, sum(amount) paid
  from {{ source('silver','payments') }}
  where not is_deleted and status='success' group by order_id
), refunds as (
  select p.order_id, sum(r.amount) refunded
  from {{ source('silver','refunds') }} r
  join {{ source('silver','payments') }} p on r.payment_id=p.payment_id
  where not r.is_deleted and not p.is_deleted and r.status='success' and p.status='success'
  group by p.order_id
)
select o.order_id, to_date(from_utc_timestamp(o.ordered_at,'Asia/Seoul')) order_date,
       coalesce(p.paid,0) paid, coalesce(r.refunded,0) refunded,
       coalesce(p.paid,0)-coalesce(r.refunded,0) net_paid,
       current_timestamp() mart_built_at
from {{ source('silver','orders') }} o
left join payments p on o.order_id=p.order_id
left join refunds r on o.order_id=r.order_id
where not o.is_deleted
```

위에서부터 이렇게 읽습니다.

1. `payments` CTE: 삭제되지 않은 성공 결제를 주문별로 합산합니다. 결과는 주문당 한 행입니다.
2. `refunds` CTE: 환불을 결제에 조인해 `order_id`를 얻고 환불과 결제가 모두 삭제되지 않은 성공 건일 때만 주문별로 합산합니다. 역시 주문당 한 행입니다.
3. 마지막 SELECT: 주문을 기준으로 두 CTE를 `left join`해 결제·환불이 없는 주문도 빠지지 않게 하고 비어 있으면 `coalesce`로 0을 넣습니다. `mart_built_at`은 테이블을 만든 시각이며 공통 환경에서는 UTC로 기록됩니다.

`refunds` CTE에 조인이 들어간 이유는 Silver `refunds`에 `order_id`가 없고 `payment_id`만 있기 때문입니다. 그래서 `payments`와 `payment_id`로 조인해 `order_id`를 찾아오며 이 조인이 환불을 두 번 세지 않으려면 Silver `payments`에 `payment_id`가 한 행씩만 있어야 합니다(4절).

[`mart_daily_settlement.sql`](../../dbt/models/mart_daily_settlement.sql) 전체입니다.

```sql
select order_date, count(*) order_count, sum(paid) paid,
       sum(refunded) refunded, sum(net_paid) net_paid,
       max(mart_built_at) mart_built_at
from {{ ref('fct_order_settlement') }}
group by order_date
```

`ref`로 fct를 읽어 `order_date`별로 건수와 금액을 다시 합산합니다. fct가 이미 주문당 한 행이라 여기서는 조인 없이 더하기만 하면 되고 `mart_built_at`은 fct에 찍힌 생성 시각의 최댓값입니다.

## 6. 데이터 테스트

### 종류

| 종류 | 형태 | 통과 조건 |
|---|---|---|
| generic | `unique`, `not_null`, `accepted_values`, `relationships`를 YAML에 붙임 | 규칙을 어긴 행이 없음 |
| singular | 규칙을 어긴 행을 반환하는 SELECT 파일 | 0행이면 통과 |

데이터 테스트는 지금 만들어진 테이블의 데이터를 검사합니다. dbt에는 고정 입력으로 모델 로직만 확인하는 unit 테스트도 있지만 이번 공통 환경(dbt-core 1.12.5 + dbt-spark 1.9.3)에서 시도했을 때 실행 오류가 나서 이번 회차에서는 사용하지 않습니다. [data tests](https://docs.getdbt.com/docs/build/data-tests)

### 우리 프로젝트의 테스트 여섯 개

generic 테스트 다섯 개는 [`sources.yml`](../../dbt/models/sources.yml)에 붙어 있습니다. 이 파일에는 Silver 원천 선언과 Gold 모델 설명·테스트가 함께 들어 있습니다(모델이 늘면 원천 선언과 모델 설명을 다른 파일로 나누기도 합니다).

```yaml
models:
  - name: fct_order_settlement
    description: Order-date basis net payments, not accounting revenue.
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: order_date
        tests: [not_null]
  - name: mart_daily_settlement
    columns:
      - name: order_date
        tests: [unique, not_null]
```

여섯 번째는 singular 테스트 [`tests/settlement_balance.sql`](../../dbt/tests/settlement_balance.sql)로, 규칙을 어긴 행을 찾는 SELECT이며 0행이면 통과입니다.

```sql
select * from {{ ref('fct_order_settlement') }}
where net_paid != paid-refunded or paid < 0 or refunded < 0
```

| 테스트 | 보장하는 것 | 보장하지 못하는 것 |
|---|---|---|
| fct `order_id` unique·not_null | 주문당 한 행 | 금액이 맞는지 |
| fct `order_date` not_null | 날짜가 비지 않음 | 시간대가 맞는지 |
| mart `order_date` unique·not_null | 하루 한 행 | 합계가 맞는지 |
| [`settlement_balance`](../../dbt/tests/settlement_balance.sql) | `net_paid = paid − refunded`, `paid`·`refunded` ≥ 0 | 조인으로 부푼 금액, 순결제액이 음수가 되는 초과 환불 |

실측: `dbt test` → `PASS=6 WARN=0 ERROR=0 SKIP=0`. 통과는 우리가 정의한 조건까지만 보장합니다.

### 지금 테스트로 잡을 수 있을까요

| 틀릴 수 있는 방식 | 지금 6개로 | 추가할 테스트 (설계) |
|---|---|---|
| 조인 fan-out으로 결제액 2배 | 못 잡음 | 환불이 2건 이상인 검증 입력의 기댓값 비교, 또는 같은 주문 범위에서 Silver의 삭제되지 않은 성공 결제 합계와 fct `paid` 합계 비교(singular) |
| 없는 결제를 가리키는 환불 | 못 잡음 | 원천 `relationships`: refunds.payment_id → payments.payment_id |
| 예상 밖 status 값 | 못 잡음 | 원천 `accepted_values`: payments·refunds의 status |
| 환불이 결제보다 큼 | 못 잡음 | singular: `refunded > paid`인 행 |

fan-out 실수가 들어가도 `order_id`는 여전히 주문당 한 행이고 `net_paid = paid − refunded`도 그대로 성립하므로 지금 테스트는 모두 통과합니다(코드로 추론, 실행하지 않음).

이 실수를 잡으려면 fan-out이 실제로 일어나는 입력이 필요합니다. 공통 예제 주문은 하나뿐인 환불(30,000원)이 철회돼 실제로 셀 환불이 없으므로 잘못된 조인을 써도 이 주문에서는 행이 복제되지 않고 결제액이 그대로 100,000원입니다. 그래서 공통 예제 기댓값만으로는 이 실수를 잡지 못합니다. 대신 환불이 두 건 이상인 검증용 주문을 넣고 기댓값과 비교하거나, Silver의 삭제되지 않은 성공 결제 합계와 fct `paid` 합계가 같은지 비교하는 singular 테스트를 둡니다. 합계를 비교할 때는 fct와 같은 주문 범위, 즉 삭제되지 않은 주문에 걸린 삭제되지 않은 성공 결제만 더해야 합니다. fct가 삭제된 주문과 주문이 없는 결제를 결과에서 빼기 때문입니다.

없는 결제를 가리키는 환불과 예상 밖 status는 Gold가 아닌 원천 쪽, 곧 `sources.yml`의 Silver 테이블 열에 붙이는 테스트로 막습니다. 원천에서 막아야 Gold 모델을 만들기 전에 문제를 알 수 있습니다. 오른쪽 열은 아직 저장소에 넣지 않은 설계입니다.

### 결과 읽기

테스트 이름은 `테스트_모델_열` 형태로 만들어집니다(예: `unique_fct_order_settlement_order_id`). 실패하면 실패한 행 수가 나오고 `dbt/target/compiled/` 아래에 컴파일된 테스트 SQL이 남는데 그 SQL을 직접 실행하면 어떤 행이 문제인지 볼 수 있습니다.

## 7. table 모델과 갱신 주기

### table 모델은 매번 다시 씁니다

설치된 dbt-spark 1.9.3은 Iceberg 테이블이 이미 있으면 지우고 다시 만드는 대신 `create or replace table ... as select`로 테이블을 통째로 교체합니다. 그래서 교체하는 동안에도 테이블이 사라지지 않고 읽는 쪽은 교체 전이나 교체 후 한 상태만 봅니다. [materializations](https://docs.getdbt.com/docs/build/materializations), [Iceberg RTAS](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#replace-table-as-select)

실측: 공통 환경의 `lake.gold.fct_order_settlement`에는 snapshot이 모두 902개 있었고 최근 5개는 모두 `operation=overwrite`, `parent_id`가 비어 있었습니다. table 설정에서는 바뀐 행이 없어도 실행할 때마다 테이블 전체를 교체합니다(설치된 코드로 확인한 동작이며 변경 없는 실행을 따로 비교하지는 않았습니다). 오래된 snapshot 정리는 7회차에서 다룹니다. 이 관찰은 공통 환경에서 본 것이므로 모든 버전·설정의 일반 법칙으로 넓히지 않습니다.

### 얼마나 자주 다시 만들까요

| 선택 | 신선도 | 비용·주의 |
|---|---|---|
| DAG를 켜면: 5분마다 Silver→`dbt build` 한 작업 (코드) | 최대 5분 + 실행 시간 | 하루 최대 288번 전체 교체 |
| 제안: Silver 5분 / Gold 30분 분리 (설계) | 30분 + 실행 시간 + Silver 반영 지연 | 하루 48번. 신선도 요구와 다시 대조 |
| 더 빨라야 한다면 | 분 단위 | 증분 모델·다른 실행 방식 검토 |

Airflow는 선택 프로필(`orchestration`)이라 기본으로는 꺼져 있습니다. 켜고 [`lakehouse_refresh`](../../airflow/dags/lakehouse_refresh.py) DAG를 활성화하면 5분마다 Silver 갱신과 `dbt build`를 한 작업으로 실행합니다([운영 방법](../../docs/operations.md)). Gold 30분 분리는 이번 회차의 설계안으로 아직 구현하지 않았고 주기는 ① 쓰임에서 정한 '얼마나 빨리 봐야 하나'에서 나옵니다. 다만 30분마다 실행한다고 숫자가 30분 안에 최신이 되지는 않습니다. 실행 시간과 Silver 반영 지연이 더해지므로 요구가 '30분 안'이라면 세 가지를 더해서 다시 확인합니다.

### 증분 모델 예고

| table (지금) | incremental (6회차) |
|---|---|
| 매번 전체를 다시 계산, 단순하고 결과가 분명 | 바뀐 부분만 반영 |
| 데이터가 크면 비쌈 | '바뀐 행'을 찾는 기준 필요, 삭제·늦은 정정까지 처리 |

6회차에서는 바뀐 것만 반영하면서도 과거 정정과 삭제를 놓치지 않으려면 무엇이 필요한지 다룹니다. [incremental models](https://docs.getdbt.com/docs/build/incremental-models)

## 8. 협업을 위한 약속: 설명·계보·소유·계약

거버넌스를 거창한 제도로 보기보다 협업하려고 코드 안에 남겨 두는 약속으로 보면 됩니다. 아래 기능은 저장소에 새로 적용하거나 실행하지 않은 설계 예시입니다.

| 약속 | 남기는 방법 | 예 |
|---|---|---|
| 설명 | 모델·열의 `description` | 무엇인지와 무엇이 아닌지 (`not accounting revenue`) |
| 계보 | `source` → `ref` 연결, `dbt ls`·`dbt docs` | 어떤 원천에서 어떤 마트가 나오는지 |
| 소유 | `groups`, `meta` | 숫자가 이상할 때 누구에게 묻는지 |
| 계약 | `contract: {enforced: true}` + 열 `data_type` | 하류가 기대하는 열 이름·타입 |

모델 계약을 켜면 dbt가 빌드 전에 모델의 열 이름과 타입이 YAML과 같은지 확인하고 다르면 모델을 만들지 않습니다. 계약을 켜면 모든 열을 적어야 합니다.

```yaml
models:
  - name: fct_order_settlement
    config:
      contract: {enforced: true}
    columns:
      - {name: order_id, data_type: string}
      - {name: net_paid, data_type: bigint}
      # ... 나머지 열도 모두 적어야 합니다
```

주의할 점이 있습니다. 설치된 dbt-spark 1.9.3 코드를 보면 계약의 열 이름·타입 사전 확인은 동작하지만 `not_null` 같은 제약(constraints)은 Spark에서 모두 '강제되지 않음'으로 되어 있습니다. 제약을 YAML에 적어도 Iceberg 테이블이 그 규칙을 막아 주지는 않습니다(코드 확인, 실행하지 않음). 계약은 열 모양을 약속하고 데이터 내용은 여전히 데이터 테스트가 확인합니다. [model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts), [documentation](https://docs.getdbt.com/docs/build/documentation), [groups](https://docs.getdbt.com/docs/build/groups)

## 9. 집에서 조회만 해 보기 (선택)

자기 Docker 환경에서 공통 예제가 이미 실행돼 있다면(bootstrap은 Silver·Gold/dbt 작업도 실행합니다) 아래 명령으로 조회만 해 볼 수 있습니다. 테이블을 새로 만들거나 바꾸지는 않지만 실행 과정에서 `dbt/target` 같은 로컬 산출물이나 잠금 파일은 생길 수 있습니다. 명령은 저장소 루트에서 한 줄씩 붙여 넣으세요. 여러 줄을 한꺼번에 붙여 넣으면 앞 명령이 뒤 줄을 입력으로 읽어 버릴 수 있습니다.

모델·원천·테스트 목록:

```bash
./scripts/lab.sh exec -T runner dbt ls --project-dir dbt --profiles-dir dbt --output name
```

`source`가 실제 테이블 이름으로 바뀐 SQL:

```bash
./scripts/lab.sh exec -T runner dbt compile --select fct_order_settlement --project-dir dbt --profiles-dir dbt
```

데이터 테스트 실행(조회만 함):

```bash
./scripts/lab.sh exec -T runner dbt test --project-dir dbt --profiles-dir dbt
```

Gold 일별 마트 조회:

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT * FROM lake.gold.mart_daily_settlement ORDER BY order_date"
```

Gold fct 테이블의 snapshot 수:

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT COUNT(*) FROM lake.gold.fct_order_settlement.snapshots"
```

Gold fct 테이블의 최근 snapshot 5개(어떤 작업으로 생겼는지):

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT committed_at, operation, parent_id FROM lake.gold.fct_order_settlement.snapshots ORDER BY committed_at DESC LIMIT 5"
```

기대하는 출력은 이렇습니다. 목록에 모델 2개·원천 3개·테스트 6개가 보이고 compile 결과에 `from silver.payments`가 보이며 Gold가 만들어져 있다면 `dbt test`는 `PASS=6`이 나옵니다(공통 환경 실측 기준). 최근 snapshot의 `operation`은 공통 환경에서 `overwrite`였고 행 수·날짜·snapshot 수는 환경마다 다릅니다. Gold 테이블이 없다는 오류가 나면 아직 dbt를 실행하지 않은 환경일 수 있으니 오류와 마지막 명령을 남겨 질문해 주세요. 결과를 맞추려고 테이블이나 볼륨을 지우지는 않습니다.

## 10. 내 프로젝트로 옮기기

내 도메인의 지표 하나를 골라 Gold 설계 순서대로 적습니다. 근거가 없는 칸은 비워 두고 '모름, 원천에서 무엇을 확인해야 함'이라고 적으면 충분합니다.

| 묶음 | 내 도메인으로 적을 것 |
|---|---|
| ① 쓰임·정의 | 누가·무슨 판단·얼마나 자주 / 지표 하나와 '아닌 것' |
| ② 단위·시간 | 한 행은 무엇 / 어떤 날짜, 어느 시간대 |
| ③ 집계 대상·조인 | 셀 행의 조건 / 1:N에서 부풀지 않는 조인 순서 |
| ④ 테스트 | 틀릴 수 있는 방식 세 가지와 막을 테스트 |
| ⑤ 갱신·관리 | 다시 만드는 주기·방식 / 설명 한 줄·소유자 |

같은 SQL을 이름만 바꿔 복사하기보다 내 지표의 정의·단위·조건을 왜 그렇게 정했는지 설명할 수 있으면 됩니다.

## 참고 문서

| 주제 | 문서 |
|---|---|
| Gold 층의 역할 | [Databricks 메달리온 아키텍처](https://docs.databricks.com/aws/en/lakehouse/medallion) |
| dbt 개요·ref·source | [dbt 소개](https://docs.getdbt.com/docs/introduction) · [ref](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) · [sources](https://docs.getdbt.com/docs/build/sources) |
| dbt build·materialization | [dbt build](https://docs.getdbt.com/reference/commands/build) · [materializations](https://docs.getdbt.com/docs/build/materializations) |
| 데이터 테스트 | [data tests](https://docs.getdbt.com/docs/build/data-tests) · [unit tests](https://docs.getdbt.com/docs/build/unit-tests) |
| 증분 모델 | [incremental models](https://docs.getdbt.com/docs/build/incremental-models) · [incremental strategy](https://docs.getdbt.com/docs/build/incremental-strategy) · [Spark 설정](https://docs.getdbt.com/reference/resource-configs/spark-configs) |
| 설명·소유·계약 | [documentation](https://docs.getdbt.com/docs/build/documentation) · [groups](https://docs.getdbt.com/docs/build/groups) · [model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) |
| dbt-spark 1.9.3 | [PyPI](https://pypi.org/project/dbt-spark/1.9.3/) · 소스: [table materialization](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/materializations/table.sql) · [adapters.sql](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/adapters.sql) · [impl.py](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/adapters/spark/impl.py) |
| Spark 시간대 | [from_utc_timestamp](https://spark.apache.org/docs/3.5.6/api/sql/index.html#from_utc_timestamp) · [Spark SQL 설정](https://spark.apache.org/docs/3.5.6/configuration.html#runtime-sql-configuration) |
| Iceberg | [Replace table as select](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#replace-table-as-select) · [Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/) |

dbt-spark 소스 링크는 읽기 편하도록 GitHub 저장소를 가리킵니다. 이 가이드의 동작 설명은 공통 환경에 설치된 dbt-spark 1.9.3 패키지 파일을 직접 읽어 확인했습니다.
