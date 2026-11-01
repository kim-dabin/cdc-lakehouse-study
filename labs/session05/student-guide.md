# 5회차 학생 가이드: 현재 상태에서 믿을 수 있는 지표로

지난 시간에는 변경 이력으로 환불 한 건의 현재 상태를 Silver로 만들었습니다. 이번 시간에는 그 현재 상태 위에 여러 마트가 함께 쓰는 Silver core(공통 fact·dim)를 만들고, Silver core로 정산 담당자가 볼 Gold 지표를 만듭니다. SQL을 쓰기 전에 숫자가 무엇을 뜻하는지 먼저 정하고 숫자가 틀렸을 때 알아챌 테스트도 함께 설계합니다.

이번 회차는 설계와 코드 읽기가 중심이라 수업 중에 새 실습을 실행하지 않고 저장소의 `dbt/` 코드와 공통 환경에서 미리 확인한 출력을 함께 읽습니다. 집에서 조회만 하는 명령을 실행해 보고 싶다면 [9절](#9-집에서-조회만-해-보기-선택)을 참고하세요. 새로 작성해서 제출할 파일은 없으며 과제 형식과 기한은 별도 공지를 따릅니다.

이 가이드에서 쓰는 표시는 세 가지입니다.

- 실측: 공통 환경에서 확인한 값이고 날짜를 함께 적습니다. 10/31 값은 조회만 해서 확인했고 11/1 값은 Silver core를 추가한 뒤 파이프라인을 실행해서 확인했습니다. 여러분 환경에서는 행 수·시각·snapshot 수가 다를 수 있습니다.
- 코드: 저장소 파일에 적힌 내용입니다.
- 설계: 실행으로 확인하지 않은 제안입니다.

## 층마다 맡는 일

| 층 | 이 과정의 구성 | 맡는 일 |
|---|---|---|
| Bronze | Flink가 토픽마다 하나씩 쓰는 Iceberg 테이블(`lake.bronze.orders`·`payments`·`refunds`) | 변경 원문을 그대로 append, 재처리 출발점 |
| Silver 현재 상태 | Spark SQL MERGE(`scripts/pipeline.py`), `lake.silver.orders`·`payments`·`refunds` | 원천 테이블 구조 그대로 키당 한 행, 삭제 표시(`is_deleted`) 보존 (4회차) |
| Silver core | dbt 모델 → Spark → Iceberg, `lake.silver.dim_order`·`fct_payment`·`fct_refund` | 삭제 제외, 공통 키(환불 → 결제 → 주문), 서울 기준 주문일. 가장 작은 입도의 공통 fact·dim |
| Gold | dbt 모델 → Spark → Iceberg, `lake.gold.mart_order_settlement`·`mart_daily_settlement` | 소비자에게 맞춘 서빙 테이블과 집계 마트. 지표 규칙(성공 건만, 결제 − 환불) |

공통 환경의 Bronze는 2026-11-01부터 토픽마다 테이블 하나로 나뉘었습니다. 이전 회차 자료에 나오는 `lake.bronze.cdc_events`는 세 토픽을 한 테이블에 담던 이전 구조입니다. 쌓여 있던 이력은 토픽별 테이블로 옮겼고 옮긴 내용이 원래 테이블과 같은지 체크섬으로 확인했습니다(실측 11/1). 입력이 같으니 Silver를 다시 만들어도 같은 결과가 나옵니다. 다만 공통 환경의 Silver는 10/23 이후 갱신하지 않은 상태였기 때문에 11/1에 파이프라인을 실행하면서 10/23~10/25에 Bronze로 들어온 이벤트가 함께 반영됐습니다. 그래서 10/31 기록보다 행 수가 늘었습니다(Silver 주문 19 → 27, Gold 10월 19일 주문 15 → 23건). 4회차 실습의 `lake.lesson04.<RUN_ID>_bronze`처럼 Bronze 한 테이블이 토픽 하나를 담는 모양입니다.

### fact·dim은 어느 층에 둘까: 두 패턴

| 구분 | A. Gold에 fact·dim | B. Silver에 fact·dim (이 과정) |
|---|---|---|
| Silver | 정제·중복 제거, 원천 구조 유지 | 원천 정리 + 공통 fact·dim(가장 작은 입도) |
| Gold | fact·dim(star schema) + 마트 | 서빙용 넓은 테이블, 집계 마트 |
| 잘 맞는 곳 | 도메인 하나, BI가 fact·dim을 직접 조인 | 여러 마트가 같은 주문·결제·환불 기준을 함께 씀 |
| 치르는 비용 | 마트마다 삭제 제외·키 연결·날짜 규칙을 반복하기 쉬움 | Silver 모델이 늘고 공통 규칙을 먼저 합의해야 함 |

[Databricks 메달리온 문서](https://docs.databricks.com/aws/en/lakehouse/medallion)(2026-10-29 갱신)는 Gold를 "where you'll model your data for reporting and analytics using a dimensional model"이라고 설명하고, Silver에는 "at least one validated, non-aggregated representation of each record"를 두라고 합니다. A에 가까운 설명입니다. 두 패턴 모두 쓰이는 설계이고 어느 쪽이 정답이라기보다 무엇을 다시 쓰고 싶은지에 따라 고릅니다.

이 과정은 B를 고릅니다. 정산 마트뿐 아니라 환불 분석이나 결제 실패 분석 같은 다른 마트도 같은 주문·결제·환불 기준을 써야 한다고 보기 때문입니다. 그래서 Silver를 두 단계로 나눕니다. 4회차의 현재 상태 테이블은 원천 구조와 삭제 표시를 그대로 두고, 그 위에 dbt로 Silver core를 만듭니다. Gold는 Silver core만 읽어 소비자용 테이블을 만듭니다. Databricks 문서가 말하는 "non-aggregated representation"은 두 단계 모두에 해당합니다(설계 해석).

이름도 층에 맞췄습니다. Silver core 모델은 `dim_`·`fct_`로, Gold 모델은 `mart_`로 시작합니다. 주문 단위 Gold 테이블은 2026-11-01에 `fct_order_settlement`에서 `mart_order_settlement`로 이름을 바꿨고 바꾸기 전후 값이 같은 것을 확인했습니다(실측(11/1), 생성 시각 열 `mart_built_at` 제외). 이전 회차 자료와 기록에 나오는 `fct_order_settlement`는 같은 역할의 이전 이름입니다.

Silver core와 Gold는 Silver 현재 상태를 읽어 다시 만듭니다. 4회차에서 본 것처럼 MERGE로 갱신하는 Silver는 append만 하는 테이블과 달리 새로 붙은 행만 골라 이어 읽기 어렵습니다. 그래서 이번 회차의 Silver core와 Gold는 가장 단순한 방법인 배치로 시작합니다. 정해진 주기마다 Silver 현재 상태 전체를 다시 읽어 테이블을 새로 만들고 공통 예제는 Gold 주기를 30분으로 가정합니다(설계, 7절). 바뀐 부분만 골라 반영하는 증분 방식은 6회차에서 무엇이 필요한지와 함께 다룹니다.

## Silver core와 Gold 설계 순서: 아홉 가지 질문

4회차의 Silver 설계 순서처럼 이번에도 답보다 질문 순서를 가져갑니다. 단계마다 답과 근거, 그 답이 깨지는 조건을 함께 적고 근거가 없으면 칸을 비워 둔 채 확인할 원천 정보를 적습니다. 마지막 열은 그 답을 어느 층에서 정하는지입니다. 여러 마트가 같이 써야 하는 규칙은 Silver core에, 한 소비자의 지표 규칙은 Gold에 둡니다.

| 묶음 | 물을 것 | 공통 예제의 답 | 깨지는 조건·확인할 것 | 정하는 층 |
|---|---|---|---|---|
| ① 쓰임·정의 | 누가 어떤 판단에, 얼마나 자주 보나요? | 정산 담당자, 주문일별 순결제액, 30분쯤 늦어도 됨 (설계 가정) | 요구 신선도가 바뀌면 갱신 방식이 바뀜 | Gold |
|  | 무엇을 더하고 빼나요? 무엇이 아닌가요? | 성공 결제 − 성공 환불(철회 제외). 회계 매출 아님 | 부서마다 '매출' 정의가 다름 | Gold |
| ② 단위·시간 | 한 행은 무엇인가요? | Silver core: 결제 1건, 환불 1건, 주문 1건 / Gold: 주문 1건, 주문일 하루 | 단위가 섞이면 합계가 중복됨 | 둘 다 |
|  | 어떤 날짜, 어느 시간대인가요? | 주문일, 서울 기준 (`dim_order`에서 고정) | Spark 세션 시간대가 바뀌면 날짜가 달라짐 | Silver core |
| ③ 집계 대상·조인 | 어떤 행을 세나요? | 삭제 제외(Silver core), `status='success'`(Gold) | 상태 값이 늘면 조건 재검토 | 둘 다 |
|  | 어떻게 묶어야 숫자가 부풀지 않나요? | 결제·환불을 주문 단위로 먼저 집계한 뒤 `dim_order`에 붙임 | Silver core에 키가 중복되면 조인이 부풂 | Gold |
| ④ 테스트 | 어떻게 틀릴 수 있고 무엇으로 막나요? | unique·not_null·relationships·규칙 | 테스트 통과는 정의한 조건만 보장 | 둘 다 |
| ⑤ 갱신·관리 | 얼마나 자주, 어떤 방식으로 다시 만드나요? | table 전체 교체, Gold 30분 (설계) | 데이터가 커지면 증분 검토(6회차) | 둘 다 |
|  | 누가 소유하고 무엇을 약속하나요? | 설명·계보·소유, 열 계약 (설계) | 계약은 열 모양만, 내용은 테스트 | 둘 다 |

정산이 아닌 도메인이어도 질문은 같습니다. 영상 시청 로그라면 집계 대상은 '테스트 계정·봇 제외'가 되고 '사용자 상태 이력과 조인하면 시청 수가 복제된다'는 점이 fan-out 주의점이 됩니다. 예약 도메인이라면 날짜를 접수일로 볼지 완료일로 볼지부터 정합니다. 그 날짜 규칙을 여러 마트가 함께 써야 한다면 Silver core에 둡니다.

## 1. dbt는 무엇이고 언제 쓰나요

### 무엇인가요

dbt는 이미 들어온 데이터를 엔진 안에서 바꾸는 변환(T) 도구입니다. SELECT 문으로 변환 로직을 쓰면 dbt가 그것을 실제 SQL로 컴파일해 엔진에 보내고 테이블 생성(DDL), 실행 순서, 테스트, 문서를 챙깁니다. 공식 문서는 dbt를 두 부분으로 나눠 설명합니다. 하나는 우리가 직접 쓰는 언어(SQL·Jinja·YAML·테스트)이고 다른 하나는 프로젝트를 컴파일해 실행 순서대로 돌리고 메타데이터를 만드는 엔진입니다.

이 과정에서 dbt가 맡는 구간은 Silver 현재 상태 → Silver core → Gold입니다. 실제 계산과 Iceberg 쓰기는 Spark Thrift Server가 합니다.

### 왜 쓰나요

| dbt가 대신 해 주는 일 | 공통 예제에서는 (코드) |
|---|---|
| SELECT만 쓰면 테이블·뷰 생성(materialization) | 모델 파일에 CREATE 문이 없고 모든 모델이 table(Iceberg) |
| `ref`·`source`로 실행 순서와 계보(lineage) | Silver core(`dim_order`, `fct_payment`, `fct_refund`)를 먼저, Gold(주문 마트 → 일별 마트)를 나중에 실행 |
| 테스트·설명을 SQL 옆에 | 데이터 테스트 15개(Silver core 9, Gold 6), `_silver__models.yml`·`_gold__models.yml`의 설명 |
| Git 브랜치·리뷰·CI 흐름 | SQL과 테스트를 한 PR로 함께 리뷰 (8절) |
| 증분 모델로 바뀐 부분만 반영 | 6회차 주제, 지금은 매번 전체 교체 |

공식 문서는 몇 번을 다시 실행해도 안전하고 같은 결과를 내는(idempotent) 변환을 만들라고 권합니다. 이번 회차의 Silver core와 Gold가 매번 Silver 현재 상태 전체를 읽어 테이블을 새로 만드는 것도 그 방향입니다.

### 주의할 점

- dbt는 계산 엔진·스케줄러·수집 도구가 아닙니다. 계산은 Spark, 주기 실행은 Airflow(선택 프로필), 수집은 Flink가 맡습니다.
- Jinja가 한 겹 끼어 있어서 실제로 실행된 SQL은 `target/compiled` 아래 컴파일 결과로 확인합니다.
- 데이터 테스트는 테이블을 만든 다음에 검사합니다. 테스트가 실패하면 하류 모델은 SKIP되지만 테스트한 모델의 테이블은 이미 새 내용으로 바뀌어 있습니다(2절).
- 다시 실행해도 같은 결과가 나오는지는 dbt가 아니라 모델을 어떻게 쓰느냐에 달려 있습니다. `mart_order_settlement`의 `mart_built_at`은 `current_timestamp()`로 채우기 때문에 실행할 때마다 바뀌고 숫자 열(paid·refunded·net_paid)은 Silver가 같으면 같습니다(코드).
- 엔진마다 어댑터가 따로 있고 지원 기능이 다릅니다. 공식 문서 기준으로 새로 설치할 때 기본값인 dbt v2(Rust 엔진)에서 Apache Spark 연결은 아직 Beta입니다. 이 과정은 v1 계열인 dbt-core 1.12.5와 dbt-spark 1.9.3을 씁니다(실측).
- table 모델은 실행할 때마다 전체를 다시 쓰므로 데이터가 커지면 비용이 늘어납니다. 증분 모델로 바꾸려면 키와 늦게 도착한 데이터 처리까지 설계해야 합니다(6회차).

### 언제 고르나요

| dbt가 맞을 때 | 다른 방법이 나을 때 |
|---|---|
| SQL로 쓸 수 있는 배치 변환 | 초 단위 실시간 반영: Flink SQL 같은 스트리밍 처리(2·3회차) |
| 모델이 여럿이고 서로 의존함 | 모델이 두세 개뿐: Spark SQL 파일을 순서대로 실행하는 스크립트 |
| 테스트·설명·리뷰를 코드와 함께 관리해야 함 | 절차적 로직, ML, 세밀한 Spark 튜닝: PySpark |
| 여러 사람이 같은 변환을 함께 고침 | 쓰는 엔진의 dbt 어댑터가 없거나 아직 미성숙함 |

이 저장소도 이렇게 나눠 씁니다. Kafka → Bronze 적재는 Flink SQL, Bronze → Silver 현재 상태 MERGE는 `scripts/pipeline.py`의 Spark SQL, Silver 현재 상태 → Silver core → Gold는 dbt입니다. Silver MERGE도 dbt 증분 모델(merge 전략)로 옮길 수는 있습니다. 다만 LSN·오프셋 순서로 최신 행을 고르고 삭제를 표시로 남기는 규칙을 다시 써야 하고 처음 만들 때와 이어서 반영할 때를 모두 테스트해야 합니다. 6회차에서 증분을 다룰 때 다시 봅니다.

### 프로젝트 파일

저장소 `dbt/` 폴더의 파일별 역할입니다.

| 파일 | 맡는 일 | 우리 프로젝트 (코드) |
|---|---|---|
| [`dbt_project.yml`](../../dbt/dbt_project.yml) | 프로젝트 이름·기본 설정 | 모든 모델 `materialized: table`, `file_format: iceberg`. 층마다 폴더 하나: `models/silver/`는 `+schema: silver`, `models/gold/`는 `+schema: gold` |
| [`profiles.yml`](../../dbt/profiles.yml) | 접속 정보 | Spark Thrift(`spark:10000`), 기본 schema `gold` |
| [`macros/generate_schema_name.sql`](../../dbt/macros/generate_schema_name.sql) | 스키마 이름 규칙 | 모델에 지정한 스키마 이름(`silver`, `gold`)을 그대로 씀 |
| [`models/sources.yml`](../../dbt/models/sources.yml) | 원천 선언 | dbt 밖에서 `pipeline.py`가 만든 Silver 현재 상태 세 테이블 |
| [`models/silver/*.sql`](../../dbt/models/silver/) | Silver core 모델 | `dim_order`, `fct_payment`, `fct_refund` |
| [`models/silver/_silver__models.yml`](../../dbt/models/silver/_silver__models.yml) | Silver core 설명과 테스트 | 테스트 아홉 개 |
| [`models/gold/*.sql`](../../dbt/models/gold/) | Gold 모델 | `mart_order_settlement`, `mart_daily_settlement` |
| [`models/gold/_gold__models.yml`](../../dbt/models/gold/_gold__models.yml) | Gold 설명과 테스트 | 테스트 다섯 개 |
| [`tests/*.sql`](../../dbt/tests/settlement_balance.sql) | 규칙을 어긴 행을 찾는 SELECT | `settlement_balance` |

모델 파일에는 CREATE 문이 없고 테이블로 만들지 뷰로 만들지는 materialization 설정이 정합니다. 설명·테스트를 담은 YAML 파일은 dbt가 이름을 보지 않고 `models/` 아래의 `.yml`을 모두 읽습니다. 이 과정은 층마다 폴더를 두고 그 폴더의 모델 설명을 `_층__models.yml`에 모았습니다. 이름 앞의 밑줄은 폴더에서 맨 위에 보이게 하려는 관례입니다. [properties 파일](https://docs.getdbt.com/reference/define-properties) 실측(11/1): `Found 5 models, 15 data tests, 3 sources`.

스키마 이름 규칙을 따로 둔 이유가 있습니다. dbt는 기본으로 접속 정보의 스키마(`gold`) 뒤에 모델에 지정한 스키마를 붙여 `gold_silver` 같은 이름을 만듭니다. 이 과정은 Silver core를 Silver 현재 상태와 같은 `lake.silver`에, Gold를 `lake.gold`에 두려고 지정한 이름을 그대로 쓰는 매크로를 넣었습니다. [custom schemas](https://docs.getdbt.com/docs/build/custom-schemas)

최신 문서는 YAML에서 `data_tests:` 키를 쓰지만 저장소의 `tests:` 키도 이번 버전에서 그대로 인식됩니다(실측 11/1: 15 data tests).

## 2. source·ref와 dbt build

모델 SQL에는 테이블 이름 대신 함수를 씁니다.

- `{{ source('silver','orders') }}`: `sources.yml`에 선언한 원천 테이블(Silver 현재 상태)입니다. `dim_order`를 컴파일하면 `from silver.orders`가 되고(실측 11/1), Spark 기본 카탈로그가 `lake`이므로 `lake.silver.orders`와 같은 테이블입니다.
- `{{ ref('fct_payment') }}`: 다른 모델을 가리킵니다. Gold `mart_order_settlement`를 컴파일하면 `from silver.fct_payment`, `from silver.fct_refund`, `from silver.dim_order`가 됩니다(실측 11/1). ref로 연결해야 dbt가 실행 순서와 계보를 알 수 있습니다. 테이블 이름을 직접 쓰면 쿼리는 실행될 수 있어도 의존 관계가 끊깁니다.

공통 파이프라인([`scripts/pipeline.py`](../../scripts/pipeline.py))은 Silver 현재 상태를 갱신한 뒤 `dbt build`를 실행합니다. `dbt build`는 `dbt run`을 모두 끝낸 다음 `dbt test`를 따로 돌리지 않고 의존 순서대로 모델을 실행한 뒤 바로 그 모델의 테스트를 실행합니다. 11/1 공통 환경에서는 이 순서로 실행됐습니다(실측).

```text
dim_order → 테스트 3개
fct_payment → 테스트 3개 (relationships: order_id → dim_order 포함)
fct_refund → 테스트 3개 (relationships: payment_id → fct_payment 포함)
mart_order_settlement → 테스트 4개
mart_daily_settlement → 테스트 2개
Done. PASS=20 WARN=0 ERROR=0 SKIP=0
```

상류 모델의 테스트가 실패하면 그 모델에 의존하는 하류 모델은 SKIP됩니다. 예를 들어 `fct_payment`의 unique 테스트가 실패하면 `fct_payment`를 ref로 읽는 `fct_refund`, `mart_order_settlement`, `mart_daily_settlement`는 다시 만들어지지 않고 이전 테이블이 그대로 남습니다. Silver core의 테스트가 Gold를 지키는 문 역할을 하는 셈입니다.

그런데 테스트는 `fct_payment`를 만든 뒤에 실행되므로 `fct_payment`는 이미 새 내용으로 바뀐 상태입니다. 결국 Silver core 일부는 새 상태, Gold는 이전 상태로 남아 기준 시점이 어긋납니다. 남은 Gold도 최신이 아니므로 실패를 알리고 원인을 고쳐 다시 빌드하는 장치가 필요하며 이 부분은 8회차에서 운영과 함께 다룹니다. [dbt build](https://docs.getdbt.com/reference/commands/build)

실측: `dbt=1.12.5`, `spark=1.9.3`.

## 3. 지표 정의·집계 단위·시간대

### 지표를 먼저 정의합니다

| 지표 | 정의 | 아닌 것 |
|---|---|---|
| 결제액 `paid` | 주문의 성공 결제 합. 삭제된 결제는 Silver core에서 이미 빠짐 | 실패·취소된 결제 |
| 환불액 `refunded` | 성공 환불 합. 환불에 연결된 결제도 성공이어야 함. 삭제(철회)된 환불은 Silver core에서 이미 빠짐 | 철회된 환불 |
| 순결제액 `net_paid` | `paid − refunded`, 주문일 기준 | 회계 매출, 환불일 기준 정산 |

[`_gold__models.yml`](../../dbt/models/gold/_gold__models.yml)의 주문 마트 설명에는 `Gold order-level mart built from the Silver core. Order-date basis net payments per order, not accounting revenue.`라고 적혀 있습니다. 회계 매출이 아니라는 점까지 적어 두면 다른 부서 숫자와 다를 때 무엇이 다른지 바로 설명할 수 있습니다.

주문일 기준이라 10월 30일 주문이 10월 31일에 환불되면 그 환불은 10월 30일 숫자에서 빠집니다. 이미 본 과거 날짜의 숫자도 나중에 바뀔 수 있으며 이 문제는 6회차 소급 정정에서 다시 다룹니다.

### 한 행은 무엇인가요

| 층 | 테이블 | 한 행 | 한 번만 나와야 하는 키 |
|---|---|---|---|
| Silver core | `dim_order` | 주문 1건 | `order_id` |
| Silver core | `fct_payment` | 결제 1건 | `payment_id` |
| Silver core | `fct_refund` | 환불 1건 | `refund_id` |
| Gold | `mart_order_settlement` | 주문 1건 | `order_id` |
| Gold | `mart_daily_settlement` | 주문일 하루 | `order_date` |

단위가 정해지면 unique라는 테스트 규칙이 생깁니다. Silver core에 가장 작은 입도(결제 1건, 환불 1건)를 두는 이유는 다시 묶기 쉽기 때문입니다. 주문 단위 정산, 일별 마트, 결제 상태별 실패 분석처럼 묶는 기준이 다른 마트도 같은 fact에서 만들 수 있습니다. Gold에서 바로 날짜별로 모으지 않고 주문 단위 테이블을 거치는 이유도 있습니다. 주문마다 `paid − refunded`가 맞는지 확인하기 쉽고 어느 날 숫자가 이상할 때 원인 주문을 되짚을 수 있습니다.

### 날짜와 시간대

```sql
-- dbt/models/silver/dim_order.sql
to_date(from_utc_timestamp(ordered_at,'Asia/Seoul')) order_date
-- 2026-10-30 15:30 (UTC) → 2026-10-31 00:30 (서울) → 2026-10-31
```

공통 환경의 Spark는 세션 시간대가 UTC입니다([`spark/start-thrift.sh`](../../spark/start-thrift.sh), 실측으로도 확인). 이 식은 `ordered_at`을 UTC 시각으로 보고 서울 시각으로 바꾼 뒤 날짜만 남깁니다. 날짜 규칙을 `dim_order` 한 곳에서 정하기 때문에 Gold의 어느 마트든 같은 주문일을 씁니다. **내 프로젝트에 이 식을 옮길 때는 세션 시간대부터 확인하세요.** 세션 시간대가 이미 서울이면 같은 식이 다른 결과를 낼 수 있습니다. [from_utc_timestamp](https://spark.apache.org/docs/3.5.6/api/sql/index.html#from_utc_timestamp)

## 4. 셀 행 고르기와 fan-out

### 어떤 행을 셀까요

| 조건 | 정하는 곳 | 이유 |
|---|---|---|
| 삭제된 주문·결제·환불 제외 (`not is_deleted`) | Silver core 세 모델 | 어느 마트에서도 삭제된 행을 세지 않음 |
| 환불의 주문 키 찾기 (환불 → 결제 → 주문) | Silver core `fct_refund` | 환불 원천에는 `order_id`가 없고 `payment_id`만 있음 |
| 성공 결제만 (`status='success'`) | Gold | 정산 지표의 정의. 결제 실패 분석 마트라면 다른 status를 셀 수 있음 |
| 성공 환불이고 연결된 결제도 성공 (`status`, `payment_status`) | Gold | 철회·실패 환불과 성공하지 않은 결제의 환불 제외 |

4회차에서 Silver 현재 상태가 삭제를 지우지 않고 `is_deleted`로 남겨 둔 덕분에 Silver core는 철회된 환불을 알아보고 정확히 뺄 수 있습니다. 공통 예제(결제 100,000원, 환불 30,000원 철회)의 순결제액은 100,000원이고 저장소의 [`scripts/verify.py`](../../scripts/verify.py)도 실제 원천 변경 시나리오에서 환불 삭제 뒤 `net_paid=100000`을 기댓값으로 확인합니다. 삭제 표시를 거르지 않으면 70,000원으로 틀립니다.

`fct_refund`는 환불을 `fct_payment`에 `left join`해서 `order_id`를 얻습니다. 이 조인은 `fct_payment`에 `payment_id`가 한 행씩만 있다는 전제 위에 있습니다. 같은 키가 두 행이면 환불이 두 번 세집니다. 그래서 `fct_payment.payment_id`에 unique 테스트를 붙였습니다. 삭제된 결제에 걸린 환불은 `order_id`와 `payment_status`가 비어서 Gold 조건에서 빠지고, 동시에 relationships 테스트가 실패해 알려 줍니다(6절).

### 조인이 숫자를 부풀리는 경우 (fan-out)

손 계산 예시로 보겠습니다. 주문 O-1에 결제 P-1(100,000원)이 한 건 있고, 이 결제에 환불 R-1(10,000원)과 R-2(20,000원) 두 건이 걸려 있습니다.

결제와 환불을 `payment_id`로 먼저 조인하면 결과가 두 행이 됩니다. 결제는 한 건이지만 짝지을 환불이 두 건이기 때문입니다.

| 결제 | 결제 금액 | 환불 | 환불 금액 |
|---|---|---|---|
| P-1 | 100,000 | R-1 | 10,000 |
| P-1 | 100,000 | R-2 | 20,000 |

두 행 모두 결제 금액 칸에 100,000이 들어 있습니다. 이 상태에서 결제 금액을 더하면 100,000 + 100,000 = 200,000원이 나옵니다. 결제는 한 번인데 두 번 센 것입니다. 이렇게 1:N 조인 때문에 한쪽 행이 상대 행 수만큼 늘어나 합계가 부푸는 것을 fan-out이라고 합니다.

| 방법 | 결과 |
|---|---|
| 결제와 환불을 먼저 조인한 뒤 합계 | 결제 행이 2행으로 늘어 `sum(결제)=200,000` ✗ |
| 결제는 결제끼리, 환불은 환불끼리 주문 단위로 먼저 합계한 뒤 주문 한 행에 붙임 | 결제 100,000, 환불 30,000, 순결제액 70,000 ✓ |

Gold `mart_order_settlement`는 두 번째 방법을 씁니다. Silver core의 `fct_payment`와 `fct_refund`를 각각 주문별로 먼저 합친 뒤 `dim_order`에 붙입니다.

## 5. 코드 읽기

### Silver core

[`models/silver/dim_order.sql`](../../dbt/models/silver/dim_order.sql)

```sql
-- Silver core: one row per order that is not deleted.
-- The order date (Asia/Seoul) is fixed here so every Gold table uses the same date.
select order_id, ordered_at,
       to_date(from_utc_timestamp(ordered_at,'Asia/Seoul')) order_date,
       status
from {{ source('silver','orders') }}
where not is_deleted
```

[`models/silver/fct_payment.sql`](../../dbt/models/silver/fct_payment.sql)

```sql
-- Silver core: one row per payment that is not deleted (smallest grain).
select payment_id, order_id, amount, status
from {{ source('silver','payments') }}
where not is_deleted
```

[`models/silver/fct_refund.sql`](../../dbt/models/silver/fct_refund.sql)

```sql
-- Silver core: one row per refund that is not deleted (smallest grain).
-- The order key comes from the refund's payment; payment_status keeps the payment's state.
select r.refund_id, r.payment_id, p.order_id, r.amount, r.status,
       p.status payment_status
from {{ source('silver','refunds') }} r
left join {{ ref('fct_payment') }} p on r.payment_id=p.payment_id
where not r.is_deleted
```

세 모델 모두 삭제된 행을 빼고 집계하지 않습니다. `dim_order`는 주문일을 서울 기준으로 고정하고, `fct_refund`는 결제를 거쳐 주문 키를 붙입니다. status는 거르지 않고 열로 남깁니다. 어떤 status를 셀지는 마트마다 다를 수 있기 때문입니다.

### Gold

[`mart_order_settlement.sql`](../../dbt/models/gold/mart_order_settlement.sql) 전체입니다.

```sql
-- Gold mart: one row per order, built from the Silver core (renamed from mart_order_settlement on 2026-11-01).
-- Aggregate payments and refunds per order first to avoid join fan-out.
with payments as (
  select order_id, sum(amount) paid
  from {{ ref('fct_payment') }}
  where status='success' group by order_id
), refunds as (
  select order_id, sum(amount) refunded
  from {{ ref('fct_refund') }}
  where status='success' and payment_status='success'
  group by order_id
)
select o.order_id, o.order_date,
       coalesce(p.paid,0) paid, coalesce(r.refunded,0) refunded,
       coalesce(p.paid,0)-coalesce(r.refunded,0) net_paid,
       current_timestamp() mart_built_at
from {{ ref('dim_order') }} o
left join payments p on o.order_id=p.order_id
left join refunds r on o.order_id=r.order_id
```

위에서부터 이렇게 읽습니다.

1. `payments` CTE: Silver core 결제 중 성공 건을 주문별로 합산합니다. 결과는 주문당 한 행입니다.
2. `refunds` CTE: Silver core 환불 중 환불과 연결된 결제가 모두 성공인 건을 주문별로 합산합니다. `fct_refund`에 이미 `order_id`가 있어서 여기서는 조인이 필요 없습니다. 역시 주문당 한 행입니다.
3. 마지막 SELECT: `dim_order`를 기준으로 두 CTE를 `left join`해 결제·환불이 없는 주문도 빠지지 않게 하고 비어 있으면 `coalesce`로 0을 넣습니다. `mart_built_at`은 테이블을 만든 시각이며 공통 환경에서는 UTC로 기록됩니다.

삭제 제외, 주문 키 찾기, 날짜 변환이 Silver core로 옮겨 가서 Gold SQL에는 지표 규칙(성공 건만, 결제 − 환불)과 fan-out을 피하는 묶는 순서만 남았습니다. 실측(11/1): 이렇게 바꾼 뒤 Gold 두 테이블의 내용은 바꾸기 직전 빌드와 같았습니다(`mart_built_at`을 빼고 비교).

[`mart_daily_settlement.sql`](../../dbt/models/gold/mart_daily_settlement.sql) 전체입니다.

```sql
select order_date, count(*) order_count, sum(paid) paid,
       sum(refunded) refunded, sum(net_paid) net_paid,
       max(mart_built_at) mart_built_at
from {{ ref('mart_order_settlement') }}
group by order_date
```

`ref`로 주문 마트를 읽어 `order_date`별로 건수와 금액을 다시 합산합니다. 주문 마트가 이미 주문당 한 행이라 여기서는 조인 없이 더하기만 하면 되고 `mart_built_at`은 주문 마트에 찍힌 생성 시각의 최댓값입니다.

## 6. 데이터 테스트

### 종류

| 종류 | 형태 | 통과 조건 |
|---|---|---|
| generic | `unique`, `not_null`, `accepted_values`, `relationships`를 YAML에 붙임 | 규칙을 어긴 행이 없음 |
| singular | 규칙을 어긴 행을 반환하는 SELECT 파일 | 0행이면 통과 |

데이터 테스트는 지금 만들어진 테이블의 데이터를 검사합니다. `relationships`는 한 테이블의 키 값이 다른 테이블에 모두 있는지 확인합니다. 예를 들어 결제의 `order_id`가 주문 테이블에 모두 있는지 봅니다. dbt에는 고정 입력으로 모델 로직만 확인하는 unit 테스트도 있지만 이번 공통 환경(dbt-core 1.12.5 + dbt-spark 1.9.3)에서 시도했을 때 실행 오류가 나서 이번 회차에서는 사용하지 않습니다. [data tests](https://docs.getdbt.com/docs/build/data-tests), [relationships](https://docs.getdbt.com/reference/resource-properties/data-tests#relationships)

### 우리 프로젝트의 테스트 열다섯 개

Silver core 테스트 아홉 개는 [`models/silver/_silver__models.yml`](../../dbt/models/silver/_silver__models.yml)에 있습니다.

```yaml
models:
  - name: dim_order
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: order_date
        tests: [not_null]
  - name: fct_payment
    columns:
      - name: payment_id
        tests: [unique, not_null]
      - name: order_id
        tests:
          - relationships:
              arguments:
                to: ref('dim_order')
                field: order_id
  - name: fct_refund
    columns:
      - name: refund_id
        tests: [unique, not_null]
      - name: payment_id
        tests:
          - relationships:
              arguments:
                to: ref('fct_payment')
                field: payment_id
```

(설명 줄은 줄였습니다.) 저장소는 relationships 테스트의 인자를 `arguments:` 아래에 적었고 공통 환경의 dbt 1.12.5에서 그대로 동작했습니다(실측 11/1).

Gold 테스트 여섯 개 중 다섯 개는 [`models/gold/_gold__models.yml`](../../dbt/models/gold/_gold__models.yml)에 붙어 있습니다. 원천 선언은 [`models/sources.yml`](../../dbt/models/sources.yml)에 따로 있습니다.

```yaml
models:
  - name: mart_order_settlement
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
select * from {{ ref('mart_order_settlement') }}
where net_paid != paid-refunded or paid < 0 or refunded < 0
```

| 층 | 테스트 | 보장하는 것 | 보장하지 못하는 것 |
|---|---|---|---|
| Silver core | `dim_order.order_id` unique·not_null, `order_date` not_null | 주문당 한 행, 날짜가 비지 않음 | 시간대가 맞는지 |
| Silver core | `fct_payment.payment_id` unique·not_null | 결제당 한 행 (환불 조인이 부풀지 않는 전제) | 금액이 맞는지 |
| Silver core | `fct_payment.order_id` relationships → `dim_order` | 결제가 남아 있는 주문을 가리킴 | 주문이 삭제됐는데 결제는 남은 이유 |
| Silver core | `fct_refund.refund_id` unique·not_null | 환불당 한 행 | 금액이 맞는지 |
| Silver core | `fct_refund.payment_id` relationships → `fct_payment` | 환불이 남아 있는 결제를 가리킴 | 환불이 결제보다 큰지 |
| Gold | 주문 마트 `order_id` unique·not_null, `order_date` not_null | 주문당 한 행, 날짜가 비지 않음 | 금액이 맞는지 |
| Gold | 일별 마트 `order_date` unique·not_null | 하루 한 행 | 합계가 맞는지 |
| Gold | [`settlement_balance`](../../dbt/tests/settlement_balance.sql) | `net_paid = paid − refunded`, `paid`·`refunded` ≥ 0 | 조인으로 부푼 금액, 순결제액이 음수가 되는 초과 환불 |

실측(11/1): `dbt build` → `PASS=20 WARN=0 ERROR=0 SKIP=0` (모델 5개 + 데이터 테스트 15개). 10/31에 Silver core 없이 모델 2개였을 때는 `dbt test`가 `PASS=6`이었습니다. 통과는 우리가 정의한 조건까지만 보장합니다.

### 지금 테스트로 잡을 수 있을까요

| 틀릴 수 있는 방식 | 지금 15개로 | 추가할 테스트 (설계) |
|---|---|---|
| 조인 fan-out으로 결제액 2배 | 못 잡음 | 환불이 2건 이상인 검증 입력의 기댓값 비교, 또는 Silver core `fct_payment`의 성공 결제 합계와 Gold `paid` 합계 비교(singular) |
| 없는 결제를 가리키는 환불 | 잡음 (`fct_refund.payment_id` relationships) | 이미 있음 |
| 예상 밖 status 값 | 못 잡음 | Silver core `accepted_values`: `fct_payment`·`fct_refund`의 status |
| 환불이 결제보다 큼 | 못 잡음 | singular: Gold에서 `refunded > paid`인 행 |

fan-out 실수가 들어가도 `order_id`는 여전히 주문당 한 행이고 `net_paid = paid − refunded`도 그대로 성립하므로 지금 테스트는 모두 통과합니다(코드로 추론, 실행하지 않음).

이 실수를 잡으려면 fan-out이 실제로 일어나는 입력이 필요합니다. 공통 예제 주문은 하나뿐인 환불(30,000원)이 철회돼 실제로 셀 환불이 없으므로 잘못된 조인을 써도 이 주문에서는 행이 복제되지 않고 결제액이 그대로 100,000원입니다. 그래서 공통 예제 기댓값만으로는 이 실수를 잡지 못합니다. 대신 환불이 두 건 이상인 검증용 주문을 넣고 기댓값과 비교하거나, Silver core `fct_payment`의 성공 결제 합계와 Gold `paid` 합계가 같은지 비교하는 singular 테스트를 둡니다. 합계를 비교할 때는 Gold와 같은 범위, 곧 `dim_order`에 있는 주문에 걸린 성공 결제만 더해야 합니다. Gold가 `dim_order`에 없는 주문의 결제를 결과에서 빼기 때문입니다.

status 값처럼 여러 마트가 함께 기대는 규칙은 Silver core에 테스트를 붙입니다. Silver core에서 막아야 Gold를 만들기 전에 문제를 알 수 있습니다. 오른쪽 열의 '설계'는 아직 저장소에 넣지 않은 제안입니다.

### 결과 읽기

테스트 이름은 `테스트_모델_열` 형태로 만들어집니다(예: `unique_mart_order_settlement_order_id`). relationships 테스트는 대상까지 이름에 붙어 `relationships_fct_payment_order_id__order_id__ref_dim_order_`처럼 길어집니다(실측 11/1). 실패하면 실패한 행 수가 나오고 `dbt/target/compiled/` 아래에 컴파일된 테스트 SQL이 남는데 그 SQL을 직접 실행하면 어떤 행이 문제인지 볼 수 있습니다.

## 7. table 모델과 갱신 주기

### table 모델은 매번 다시 씁니다

설치된 dbt-spark 1.9.3은 Iceberg 테이블이 이미 있으면 지우고 다시 만드는 대신 `create or replace table ... as select`로 테이블을 통째로 교체합니다. 그래서 교체하는 동안에도 테이블이 사라지지 않고 읽는 쪽은 교체 전이나 교체 후 한 상태만 봅니다. Silver core 세 모델과 Gold 두 모델이 모두 이렇게 만들어집니다. [materializations](https://docs.getdbt.com/docs/build/materializations), [Iceberg RTAS](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#replace-table-as-select)

실측(10/31): 공통 환경의 주문 단위 Gold 테이블(당시 이름 `lake.gold.fct_order_settlement`)에는 snapshot이 모두 902개 있었고 최근 5개는 모두 `operation=overwrite`, `parent_id`가 비어 있었습니다. 11/1에 두 번 더 빌드한 뒤에는 904개였고, 같은 날 이름을 `mart_order_settlement`로 바꾼 뒤로는 새 테이블에 snapshot이 다시 쌓입니다(실측 11/1: 첫 빌드 뒤 1개). table 설정에서는 바뀐 행이 없어도 실행할 때마다 테이블 전체를 교체합니다(설치된 코드로 확인한 동작이며 변경 없는 실행을 따로 비교하지는 않았습니다). 오래된 snapshot 정리는 7회차에서 다룹니다. 이 관찰은 공통 환경에서 본 것이므로 모든 버전·설정의 일반 법칙으로 넓히지 않습니다.

### 얼마나 자주 다시 만들까요

| 선택 | 신선도 | 비용·주의 |
|---|---|---|
| DAG를 켜면: 5분마다 Silver 현재 상태 → `dbt build`(Silver core·Gold) 한 작업 (코드) | 최대 5분 + 실행 시간 | 하루 최대 288번 전체 교체 |
| 제안: Silver 현재 상태 5분 / Silver core·Gold 30분 분리 (설계) | 30분 + 실행 시간 + Silver 반영 지연 | 하루 48번. 신선도 요구와 다시 대조 |
| 더 빨라야 한다면 | 분 단위 | 증분 모델·다른 실행 방식 검토 |

Airflow는 선택 프로필(`orchestration`)이라 기본으로는 꺼져 있습니다. 켜고 [`lakehouse_refresh`](../../airflow/dags/lakehouse_refresh.py) DAG를 활성화하면 5분마다 Silver 갱신과 `dbt build`를 한 작업으로 실행합니다([운영 방법](../../docs/operations.md)). 30분 분리는 이번 회차의 설계안으로 아직 구현하지 않았고 주기는 ① 쓰임에서 정한 '얼마나 빨리 봐야 하나'에서 나옵니다. 다만 30분마다 실행한다고 숫자가 30분 안에 최신이 되지는 않습니다. 실행 시간과 Silver 반영 지연이 더해지므로 요구가 '30분 안'이라면 세 가지를 더해서 다시 확인합니다.

### 증분 모델 예고

| table (지금) | incremental (6회차) |
|---|---|
| 매번 전체를 다시 계산, 단순하고 결과가 분명 | 바뀐 부분만 반영 |
| 데이터가 크면 비쌈 | '바뀐 행'을 찾는 기준 필요, 삭제·늦은 정정까지 처리 |

가장 작은 입도인 Silver core의 `fct_payment`·`fct_refund`는 증분으로 바꿀 첫 후보입니다. 다만 삭제된 행을 Silver core에서 빼는 규칙이 있어서 바뀐 행만 반영할 때 사라져야 할 행도 함께 처리해야 합니다(설계). 6회차에서는 바뀐 것만 반영하면서도 과거 정정과 삭제를 놓치지 않으려면 무엇이 필요한지 다룹니다. [incremental models](https://docs.getdbt.com/docs/build/incremental-models)

## 8. 협업을 위한 약속: 설명·계보·소유·계약

거버넌스를 거창한 제도로 보기보다 협업하려고 코드 안에 남겨 두는 약속으로 보면 됩니다. 아래 기능은 저장소에 새로 적용하거나 실행하지 않은 설계 예시입니다.

| 약속 | 남기는 방법 | 예 |
|---|---|---|
| 설명 | 모델·열의 `description` | 무엇인지와 무엇이 아닌지 (`not accounting revenue`) |
| 계보 | `source` → `ref` 연결, `dbt ls`·`dbt docs` | Silver 현재 상태 → Silver core → Gold 마트 |
| 소유 | `groups`, `meta` | Silver core는 공통 기준을 맡는 팀, Gold 정산 마트는 정산 도메인 담당 |
| 계약 | `contract: {enforced: true}` + 열 `data_type` | 하류가 기대하는 열 이름·타입 |

Silver core를 여러 마트가 함께 읽기 시작하면 이 약속이 더 중요해집니다. Silver core의 열 하나를 바꾸면 그 열을 읽는 모든 Gold가 영향을 받기 때문입니다. 그래서 계약은 여러 곳에서 읽는 Silver core 모델에 먼저 거는 것을 검토합니다(설계).

모델 계약을 켜면 dbt가 빌드 전에 모델의 열 이름과 타입이 YAML과 같은지 확인하고 다르면 모델을 만들지 않습니다. 계약을 켜면 모든 열을 적어야 합니다.

```yaml
models:
  - name: fct_payment
    config:
      contract: {enforced: true}
    columns:
      - {name: payment_id, data_type: string}
      - {name: amount, data_type: bigint}
      # ... 나머지 열도 모두 적어야 합니다
```

주의할 점이 있습니다. 설치된 dbt-spark 1.9.3 코드를 보면 계약의 열 이름·타입 사전 확인은 동작하지만 `not_null` 같은 제약(constraints)은 Spark에서 모두 '강제되지 않음'으로 되어 있습니다. 제약을 YAML에 적어도 Iceberg 테이블이 그 규칙을 막아 주지는 않습니다(코드 확인, 실행하지 않음). 계약은 열 모양을 약속하고 데이터 내용은 여전히 데이터 테스트가 확인합니다. [model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts), [documentation](https://docs.getdbt.com/docs/build/documentation), [groups](https://docs.getdbt.com/docs/build/groups)

## 9. 집에서 조회만 해 보기 (선택)

자기 Docker 환경에서 공통 예제가 이미 실행돼 있다면(bootstrap은 Silver·dbt 작업도 실행합니다) 아래 명령으로 조회만 해 볼 수 있습니다. 테이블을 새로 만들거나 바꾸지는 않지만 실행 과정에서 `dbt/target` 같은 로컬 산출물이나 잠금 파일은 생길 수 있습니다. 명령은 저장소 루트에서 한 줄씩 붙여 넣으세요. 여러 줄을 한꺼번에 붙여 넣으면 앞 명령이 뒤 줄을 입력으로 읽어 버릴 수 있습니다.

먼저 확인할 것이 있습니다. 2026-11-01 이전에 만든 환경에서 새 코드를 받았다면 저장소 README의 Bronze 전환 안내를 먼저 따라야 합니다. 또 Silver core 테이블은 새 코드로 파이프라인을 한 번 실행해야 생깁니다. 그 전에는 `dbt ls`는 되지만 `dbt test`와 Silver core 조회는 테이블이 없다는 오류가 날 수 있습니다.

모델·원천·테스트 목록:

```bash
./scripts/lab.sh exec -T runner dbt ls --project-dir dbt --profiles-dir dbt --output name
```

`source`·`ref`가 실제 테이블 이름으로 바뀐 SQL:

```bash
./scripts/lab.sh exec -T runner dbt compile --select mart_order_settlement --project-dir dbt --profiles-dir dbt
```

데이터 테스트 실행(조회만 함):

```bash
./scripts/lab.sh exec -T runner dbt test --project-dir dbt --profiles-dir dbt
```

Silver core 환불 fact 조회:

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT * FROM lake.silver.fct_refund ORDER BY refund_id"
```

Gold 일별 마트 조회:

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT * FROM lake.gold.mart_daily_settlement ORDER BY order_date"
```

Gold 주문 마트의 snapshot 수:

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT COUNT(*) FROM lake.gold.mart_order_settlement.snapshots"
```

Gold 주문 마트의 최근 snapshot 5개(어떤 작업으로 생겼는지):

```bash
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "SELECT committed_at, operation, parent_id FROM lake.gold.mart_order_settlement.snapshots ORDER BY committed_at DESC LIMIT 5"
```

기대하는 출력은 이렇습니다. 목록에 모델 5개(Silver core 3개, Gold 2개)·원천 3개·테스트 15개가 보이고 compile 결과에 `from silver.fct_payment`, `from silver.fct_refund`, `from silver.dim_order`가 보입니다(실측 11/1). 테스트는 15개가 실행되고 공통 환경 11/1 빌드에서는 모두 통과했습니다. 최근 snapshot의 `operation`은 공통 환경에서 `overwrite`였고 행 수·날짜·snapshot 수는 환경마다 다릅니다. 테이블이 없다는 오류가 나면 위의 '먼저 확인할 것'을 다시 보고, 그래도 안 되면 오류와 마지막 명령을 남겨 질문해 주세요. 결과를 맞추려고 테이블이나 볼륨을 지우지는 않습니다.

## 10. 내 프로젝트로 옮기기

내 도메인을 Silver core부터 설계하고, 그 위에서 지표 하나를 골라 Gold 설계 순서대로 적습니다. 근거가 없는 칸은 비워 두고 '모름, 원천에서 무엇을 확인해야 함'이라고 적으면 충분합니다.

먼저 Silver core입니다.

| 정할 것 | 내 도메인으로 적을 것 |
|---|---|
| fact | 어떤 사건을 fact로 둘지, 한 행은 무엇인지(가장 작은 입도) |
| dim | 여러 fact가 함께 쓰는 기준 엔티티(예: 주문, 회원, 상품) |
| 공통 키 | fact에서 dim으로 가는 키, 원천에 없으면 어디를 거쳐 찾는지 |
| 공통 규칙 | 삭제 처리, 날짜·시간대처럼 모든 마트가 같아야 하는 규칙 |
| 테스트 | 키 unique·not_null, fact → dim relationships |

다음은 Gold입니다.

| 묶음 | 내 도메인으로 적을 것 |
|---|---|
| ① 쓰임·정의 | 누가·무슨 판단·얼마나 자주 / 지표 하나와 '아닌 것' |
| ② 단위·시간 | 한 행은 무엇 / Silver core의 날짜를 그대로 쓰는지 |
| ③ 집계 대상·조인 | 셀 행의 조건(지표 규칙) / 1:N에서 부풀지 않는 조인 순서 |
| ④ 테스트 | 틀릴 수 있는 방식 세 가지와 막을 테스트 |
| ⑤ 갱신·관리 | 다시 만드는 주기·방식 / 설명 한 줄·소유자 |

같은 SQL을 이름만 바꿔 복사하기보다 어떤 규칙을 Silver core에 두고 어떤 규칙을 Gold에 뒀는지, 그리고 왜 그렇게 정했는지 설명할 수 있으면 됩니다.

## 참고 문서

| 주제 | 문서 |
|---|---|
| 메달리온 층의 역할 | [Databricks 메달리온 아키텍처](https://docs.databricks.com/aws/en/lakehouse/medallion) |
| dbt 개요·ref·source | [dbt 소개](https://docs.getdbt.com/docs/introduction) · [ref](https://docs.getdbt.com/reference/dbt-jinja-functions/ref) · [sources](https://docs.getdbt.com/docs/build/sources) |
| dbt 스키마 이름 | [custom schemas](https://docs.getdbt.com/docs/build/custom-schemas) |
| dbt v1·v2와 Spark | [dbt 소개](https://docs.getdbt.com/docs/introduction)(v1·v2) · [dbt v2의 Apache Spark 연결](https://docs.getdbt.com/docs/local/connect-data-platform/spark-setup)(Beta) |
| dbt build·materialization | [dbt build](https://docs.getdbt.com/reference/commands/build) · [materializations](https://docs.getdbt.com/docs/build/materializations) |
| 데이터 테스트 | [data tests](https://docs.getdbt.com/docs/build/data-tests) · [relationships](https://docs.getdbt.com/reference/resource-properties/data-tests#relationships) · [unit tests](https://docs.getdbt.com/docs/build/unit-tests) |
| 증분 모델 | [incremental models](https://docs.getdbt.com/docs/build/incremental-models) · [incremental strategy](https://docs.getdbt.com/docs/build/incremental-strategy) · [Spark 설정](https://docs.getdbt.com/reference/resource-configs/spark-configs) |
| 설명·소유·계약 | [documentation](https://docs.getdbt.com/docs/build/documentation) · [groups](https://docs.getdbt.com/docs/build/groups) · [model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts) |
| dbt-spark 1.9.3 | [PyPI](https://pypi.org/project/dbt-spark/1.9.3/) · 소스: [table materialization](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/materializations/table.sql) · [adapters.sql](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/include/spark/macros/adapters.sql) · [impl.py](https://github.com/dbt-labs/dbt-adapters/blob/b979b343784b15f77d78c67506ad1630778db172/dbt-spark/src/dbt/adapters/spark/impl.py) |
| Spark 시간대 | [from_utc_timestamp](https://spark.apache.org/docs/3.5.6/api/sql/index.html#from_utc_timestamp) · [Spark SQL 설정](https://spark.apache.org/docs/3.5.6/configuration.html#runtime-sql-configuration) |
| Iceberg | [Replace table as select](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#replace-table-as-select) · [Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/) |

dbt-spark 소스 링크는 읽기 편하도록 GitHub 저장소를 가리킵니다. 이 가이드의 동작 설명은 공통 환경에 설치된 dbt-spark 1.9.3 패키지 파일을 직접 읽어 확인했습니다.
