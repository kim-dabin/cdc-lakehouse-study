# dbt 기반 거버넌스: 모델 계약·컬럼 리니지·데이터 계약

dbt 모델에 적은 약속(열 이름·타입·설명·테스트)을 OpenMetadata(OM)에 모아서 본다. 변환 SQL이 기준이고 OM은 그것을 읽어 보여 주는 카탈로그다. 선택 실행이며 기본 스택에는 포함하지 않는다.

## 무엇을 어디서 보장하나

| 무엇 | 어디에 적나 | 누가 확인하나 | 결과 |
|---|---|---|---|
| 열 이름·타입 (모델 계약) | `dbt/models/*/_*__models.yml`의 `contract: {enforced: true}`, `data_type` | dbt가 테이블을 만들기 **전에** 비교 | 다르면 그 모델을 만들지 않고 실패한다 |
| 데이터 내용 (키·관계·금액 규칙) | 같은 YAML의 `tests`, `dbt/tests/*.sql` | dbt가 테이블을 만든 **뒤에** 검사 | `run_results.json`에 남고 OM의 Data Quality 탭에 보인다 |
| 설명 | 같은 YAML의 `description` | 사람 리뷰 | OM 테이블·열 설명으로 들어간다 |
| 테이블·컬럼 리니지 | 모델 SQL의 `source()`·`ref()`와 컴파일된 SQL | OM dbt 수집이 컴파일된 SQL을 해석 | OM Lineage 탭에 열 단위로 보인다 |
| 데이터 계약 (OM) | `governance/openmetadata/om_api.py`가 dbt 계약에서 만든다 | OM 계약 검증 | 열·타입(schema)과 규칙(semantics)을 검사한다 |

Spark에서는 `not_null` 같은 제약 조건이 강제되지 않는다(dbt-spark 1.9.3의 `CONSTRAINT_SUPPORT`가 모두 NOT_ENFORCED). 그래서 이 저장소는 계약으로 열 모양을, 테스트로 데이터 내용을 확인한다.

## 구성

- `compose.governance.yaml`: OM 2.0.5 서버, 전용 PostgreSQL, Elasticsearch, 일회성 수집 컨테이너(`om-ingestion`). Airflow(OM 파이프라인 서비스)는 띄우지 않고 수집은 `metadata ingest`를 직접 실행한다.
- `governance/openmetadata/spark-metadata.yaml`: Spark Thrift(`spark:10000`, HiveServer2 규격, NOSASL)에서 `bronze`·`silver`·`gold` 테이블 메타데이터를 읽어 서비스 `cdc_lakehouse`로 등록한다. Hive 유형 서비스라서 OM에는 `cdc_lakehouse.default.<schema>.<table>`로 보인다.
- `governance/openmetadata/dbt.yaml`: 마지막 `dbt build`의 `manifest.json`·`run_results.json`으로 설명, 테스트 결과, 리니지(열 단위 포함)를 올린다.
- `governance/openmetadata/om_api.py`: ingestion-bot 토큰 발급, dbt 계약 → OM 데이터 계약 생성·검증, 리니지 확인.
- `scripts/governance.sh`: 위 순서를 한 번에 실행한다.

## 실행

```bash
# 1. 기본 스택이 떠 있는 상태에서 OM을 켠다 (처음에는 이미지 내려받기에 몇 분 걸린다)
./scripts/lab.sh -f compose.yaml -f compose.governance.yaml --profile governance up -d om-server

# 2. dbt build → 테이블 메타데이터 → dbt 수집 → 데이터 계약 → 리니지 확인
./scripts/governance.sh            # 마지막 dbt 결과를 그대로 쓰려면 --no-build

# 3. 브라우저: http://localhost:28585  (admin@open-metadata.org / admin, 로컬 실습용 기본값)
```

원격 컴퓨터에서 실행 중이면 `ssh -N -L 28585:127.0.0.1:28585 <host>`로 터널을 연 뒤 접속한다. 토큰을 채운 수집 설정은 `.runtime/om/`(커밋하지 않음)에 만들어진다.

메모리는 OM 서버·검색·DB를 합쳐 약 3 GB를 더 쓴다(2026-11-01 공통 환경 실측: 서버 1.3 GiB, Elasticsearch 1.4 GiB, DB 0.2 GiB). 수집 이미지(`openmetadata/ingestion:2.0.5`)는 7.7 GB라서 디스크 여유도 확인한다.

끄기: `./scripts/lab.sh -f compose.yaml -f compose.governance.yaml --profile governance stop om-server om-search om-db`

## 확인한 결과 (2026-11-01 공통 환경 실측)

- 메타데이터 수집 18건, dbt 수집 43건, 모두 성공.
- 데이터 계약 5개(Silver core 3, Gold 2) 검증 `Success`. 예: `silver.fct_payment` schema 4/4, semantics 1/1.
- 열 단위 리니지 예:
  - `silver.orders.ordered_at` → `silver.dim_order.order_date`, `ordered_at`
  - `silver.fct_payment.status` → `silver.fct_refund.payment_status`
  - `silver.fct_payment.amount` → `gold.mart_order_settlement.paid`, `net_paid`
  - `silver.fct_refund.amount` → `gold.mart_order_settlement.refunded`, `net_paid`
  - `gold.mart_order_settlement.*` → `gold.mart_daily_settlement.*` (`order_count`는 `count(*)`라 원본 열이 없다)
- 계약 위반 예: `fct_payment.amount`의 `data_type`을 `string`으로 바꾼 복사본으로 `dbt run`을 하면 `This model has an enforced contract that failed` 오류와 함께 `amount | BIGINT_TYPE | STRING_TYPE | data type mismatch`가 나오고 테이블은 바뀌지 않는다.

## 한계와 선택

- **OM 계약의 quality 항목은 비워 둔다.** dbt 테스트를 계약의 quality expectation으로 연결하면 OM이 파이프라인 서비스로 테스트를 다시 돌리려 하고, 이 실습은 Airflow를 띄우지 않아 검증이 `Aborted`가 된다(실측). dbt 테스트 결과는 dbt 수집으로 Data Quality 탭에 들어온다.
- **`catalog.json`은 쓰지 않는다.** `dbt docs generate`가 Iceberg 테이블의 열 정보를 읽지 못해 비어 있다(실측: 노드 0개). 열 타입은 계약의 `data_type`과 Spark에서 직접 읽은 메타데이터로 채운다.
- **Spark Thrift 공유.** 공통 환경의 Spark Thrift는 `singleSession=true`다. OM 수집 뒤에도 현재 데이터베이스가 `default`로 남는 것을 확인했지만, 수업 중에는 수집을 돌리지 않는다.
- **로그인 값.** `admin`/`admin`과 compose의 비밀값은 로컬 실습 기본값이다. 외부에 포트를 열지 않는다(`127.0.0.1`에만 바인드).

## 참고

- [dbt model contracts](https://docs.getdbt.com/docs/mesh/govern/model-contracts)
- [OpenMetadata dbt 연동](https://docs.open-metadata.org/v2.0.x/connectors/database/dbt)
- [OpenMetadata Hive 연결](https://docs.open-metadata.org/v2.0.x/connectors/database/hive)
- [OpenMetadata 데이터 계약 명세](https://docs.open-metadata.org/latest/how-to-guides/data-contracts/spec)
- [OpenMetadata 로컬 Docker 배포 (2.0.5)](https://docs.open-metadata.org/v2.0.x/quick-start/local-docker-deployment)
