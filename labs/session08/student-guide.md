# 8회차 학생 가이드: 설계 이유 총정리와 장애 대응

1회차부터 7회차까지 우리는 PostgreSQL 원천 변경을 Debezium과 Kafka로 수집해 Flink로 Iceberg Bronze에 적재하고, Spark SQL `MERGE`로 Silver 현재 상태를 만든 뒤, dbt로 Silver core와 Gold 정산 마트를 구축했습니다. 이어서 모델 계약·컬럼 리니지·증분 `MERGE`(`plan`·`progress`)와 Iceberg 파일·스냅샷 유지보수까지 단계별로 연결했습니다(코드·문서: [`labs/README.md`](../README.md)).

8회차는 새로운 기능을 추가하는 시간이 아닙니다. 지금까지 만든 파이프라인을 **"왜 이렇게 설계했는가(대안과 감수한 비용)"**와 **"문제가 생겼을 때 어떻게 진단하고 복구하는가(장애 대응과 재처리 시작점)"**라는 두 축으로 묶어 마무리합니다(설계). 수업 중 별도 제출 절차는 없으며, 최종 발표 일정과 과제 형식·기한은 별도 공지를 따릅니다.

이 가이드에서 쓰는 표시는 네 가지입니다.

- **실측**: 스터디 공통 환경(Spark 3.5.6, Iceberg 1.10.0, Flink 1.20, dbt-core 1.12.5, dbt-spark 1.9.3, Airflow 2.10.5)에서 실제로 실행해 확인한 값이며 날짜(KST, UTC+9)를 함께 적습니다. 개인 환경에서는 스냅샷 수, 시각, 세부 용량이 다를 수 있습니다.
- **코드**: 저장소 파일이나 오픈소스 엔진 소스에 적힌 구현입니다.
- **문서**: 저장소 운영 문서 또는 해당 버전 공식 문서에 적힌 동작과 권고 사항입니다.
- **설계**: 수업·코드에서 직접 실행하지 않은 운영 기준, 확장 방안, 개인 프로젝트 적용 기준입니다.

---

## 1. 이 회차의 목표: 새 기능 없이 총정리와 장애 대응

실무 데이터 파이프라인에서 도구를 연결해 첫 결과를 내는 것보다 더 오래 걸리는 일은 **"왜 이 구조를 골랐는지 설명하는 것"**과 **"새벽에 파이프라인이 멈췄을 때 데이터를 깨뜨리지 않고 복구하는 것"**입니다(설계).

1. **1~7회차 설계 총정리 (정답보다 설계 방법)**:
   - 수집부터 서빙·유지보수·스케줄러까지 각 층이 맡은 책임과 하지 않는 일을 한 장으로 대조합니다(코드·문서·설계).
   - 우리가 고른 단순한 방법(Bronze 전체 재조회, dbt `table` 전체 교체, Copy-on-Write, 배치 Silver 등)이 어떤 대안 대신 선택되었고, 그 대가로 어떤 비용을 감수했는지 짚습니다(코드·문서·설계).
2. **실제 장애 대응과 복구 검증**:
   - 2026년 11월 5일 스터디 공통 환경에서 발생한 디스크 포화·연쇄 중단 사고의 타임라인을 따라가며, 표면 증상(Spark 접속 실패, Connect 로그 폭증)에서 근본 원인(Bronze 빈 커밋과 `metadata.json` 누적)을 찾아내는 진단 순서를 확인합니다(스터디 공통 환경 실측 2026-11-05·2026-11-13).
   - 2026년 11월 13일에 수행한 근본 복구(속성 변경 → 스냅샷 만료 → Kafka·Connect·Flink 상류부터 재기동 → 종단 데이터 검증 → 고아 파일 삭제)의 전후 실측 숫자와 복구 순서를 익힙니다(스터디 공통 환경 실측 2026-11-13).
3. **내 프로젝트의 운영 마감**:
   - 각 층이 멈췄을 때 다시 시작하는 지점과 한계를 정리하고, 내 프로젝트의 장애 시나리오 한 줄과 최종 운영 점검표를 완성합니다(코드·문서·설계).

---

## 2. 한 장 지도: 층별로 고른 것 · 이유 · 실무에서 볼 것

1~7회차 동안 다룬 아홉 개 층·주제의 선택과 이유, 그리고 실무로 옮길 때 반드시 점검해야 할 항목입니다(실측·코드·문서·설계).

| 층 · 주제 | 이 과정에서 고른 것 (코드·실측) | 왜 이렇게 골랐나요? (설계·문서) | 실무에서 추가로 볼 것 (문서·설계) | 회차 가이드 |
|---|---|---|---|---|
| **수집 (CDC)** | PostgreSQL 논리 복제(`wal_level=logical`, `REPLICA IDENTITY FULL`, slot `cdc_lab`) + Debezium(`snapshot.mode=initial`) + Kafka 토픽 분리 (코드) | 애플리케이션 수정 없이 원천 커밋 순서(`source.lsn`)와 `UPDATE`·`DELETE` 이전 값(`before`)까지 빠짐없이 포착하기 위해 (문서·설계) | 커넥터 중단 시 replication slot이 붙잡는 미전달 WAL 누적(원천 디스크 고갈 위험), 대용량 테이블의 `REPLICA IDENTITY FULL` WAL 증가량, Kafka 보관 기간(기본 7일) (문서·설계) | [1회차](../session01/student-guide.md) · [2회차](../session02/student-guide.md) |
| **Bronze** | Flink(`cdc-bronze`)가 Kafka를 1회 읽어 토픽별 Iceberg 테이블 3개(`orders`·`payments`·`refunds`)에 원문(`payload`) + 위치 정보를 `append`, tombstone(`payload IS NULL`)만 제외, 체크포인트 완료 시 커밋 (코드·실측 2026-11-01) | 원문을 가공하지 않고 그대로 쌓아 두어야 하류 로직이 바뀌거나 버그가 나도 Kafka 보관 기간과 무관하게 처음부터 재처리할 수 있음 (문서·설계) | 유휴 시간대 빈 체크포인트 커밋 누적(`flink.max-continuous-empty-commits`), `metadata.json` 자동 삭제 속성, 스키마 변경 시 원문 JSON 호환성 (실측·문서·설계) | [3회차](../session03/student-guide.md) |
| **Silver 현재 상태** | Spark SQL 배치 `MERGE`([`scripts/pipeline.py`](../../scripts/pipeline.py)), PK별 `ROW_NUMBER`(기준: `source_lsn DESC, kafka_offset DESC`)로 후보 1건 선택, `WHEN MATCHED`에 버전 가드, 삭제는 `is_deleted=true`로 보존 (코드·실측) | 중복 전달이나 삭제 뒤 과거 `UPDATE` 재전달이 와도 현재 상태가 뒤집히지 않고, 같은 입력을 다시 실행해도 같은 결과가 나오도록 보장 (코드·실측·설계) | 다중 파티션·다중 원천 환경에서의 순서 기준(파티션 간 offset 비교 불가), 재스냅샷 시 LSN/offset 역전 처리, 과거 시점 상태 조회가 필요할 때의 SCD Type 2 여부 (문서·설계) | [4회차](../session04/student-guide.md) |
| **Silver core** | dbt `table` 모델 3개(`dim_order`, `fct_payment`, `fct_refund`), `NOT is_deleted` 필터, 환불→결제→주문 키 연결, 서울 시간대(`Asia/Seoul`) 주문일 고정 (코드·실측 2026-11-01) | 여러 마트가 공통으로 쓰는 가장 작은 입도의 fact·dim에서 삭제 제외·키 연결·표준 시간대 규칙을 한 번만 정의해 마트 간 불일치를 막음 (코드·설계) | 공통 모델을 읽는 하류 팀이 늘어날 때의 스키마 변경 합의 절차, 테이블 크기가 커질 때 전체 교체(`table`) 비용 (문서·설계) | [5회차](../session05/student-guide.md) |
| **Gold** | dbt `table` 모델 2개(`mart_order_settlement`, `mart_daily_settlement`), 결제·환불을 주문 단위로 먼저 합산한 뒤 `dim_order`에 조인, 데이터 테스트 15개(`PASS=20`) (코드·실측 2026-11-01) | 1:N 조인에서 금액이 부푸는 fan-out을 막고, 정산 담당자가 바로 쓸 순결제액(`paid - refunded`)을 검증된 상태로 제공 (코드·실측·설계) | 부서별 지표 정의(회계 매출 대 정산 순결제액) 합의, 상류 테스트 실패로 하류 모델이 `SKIP`될 때 테이블 간 기준 시점 불일치 알림 (문서·설계) | [5회차](../session05/student-guide.md) |
| **거버넌스** | 5개 dbt 모델 모두 `contract: {enforced: true}` 적용 + OpenMetadata 2.0.5 컬럼 리니지·데이터 계약 검증 (코드·실측 2026-11-01) | 데이터 테스트는 테이블 교체 뒤에 돌지만, 모델 계약은 테이블을 교체하기 전에 열 이름·타입 불일치를 차단함 (실측·코드·문서) | `dbt-spark 1.9.3`에서 `not_null` 등 제약은 `NOT_ENFORCED`이므로 값 검증은 데이터 테스트로 보완해야 함, 선택 스택(OM)의 메모리·디스크 자원 비용 (실측·코드·문서) | [6회차](../session06/student-guide.md#1-거버넌스-모델-계약과-컬럼-리니지) |
| **증분 갱신** | 격리 실험판(`lake.lesson06`)에서 Silver `create_changelog_view`로 변경 감지, 하위→상위 키 전파(`r1→p1→o1`), 쓰기 전 `plan` 고정 → 두 Gold `MERGE`(삭제·0건 날짜 `DELETE`) → 완료 후 `progress` 갱신 (코드·실측 2026-11-02) | 바뀐 주문 키와 이전·새 날짜만 고치면서도, 중간 실패 후 재시도(`s6`) 때 옛 날짜 누락 없이 전체 재계산과 `EXCEPT` 0건으로 일치시키기 위해 (실측·코드·설계) | 전체 재계산 시간 대 목표 주기를 비교해 증분 도입 여부 결정, 정기 전체 대조(reconciliation) 작업, 실패와 재시도 사이 새 스냅샷 유입 시 범위 고정 (설계) | [6회차](../session06/student-guide.md) |
| **유지보수** | `rewrite_data_files` → `expire_snapshots` → `remove_orphan_files`(`prefix_listing => true`, 24시간 이전) → `rewrite_manifests` + 테이블 속성(`write.metadata.delete-after-commit.enabled=true`) (코드·실측 2026-11-07) | 압축(`compact`)만으로는 옛 파일이 지워지지 않고 만료(`expire`)가 돌아야 삭제되며, 추적 밖 `metadata.json` 누적은 테이블 속성과 `orphans`로만 정리됨 (실측·코드·문서) | 하류 증분 소비자의 `progress` 스냅샷과 실행 중인 Flink의 최신 스냅샷 보존 하한선 설정, 유지보수 작업과 복구 작업 동시 실행 금지 (실측·문서·설계) | [7회차](../session07/student-guide.md) |
| **오케스트레이션** | [`airflow/dags/lakehouse_refresh.py`](../../airflow/dags/lakehouse_refresh.py) (`schedule='*/5 * * * *'`, `max_active_runs=1`, `catchup=False`, `is_paused_upon_creation=True`)가 내부 `runner` HTTP API 호출 (코드) | 스케줄러는 "언제·동시에 몇 개·실패 시 재호출"만 맡고, "다시 돌려도 결과가 같은가(멱등성)"는 파이프라인 작업 내부 규칙이 책임지도록 분리 (코드·문서·설계) | 컨테이너 재시작 시 runner 메모리 실행 이력 초기화 처리, 운영 환경에서의 실패 알림과 재시도(`retries`) 정책 (코드·문서·설계) | [6회차](../session06/student-guide.md#airflow-30분-주기-스케줄과-멱등성-코드문서설계) · 본 문서 5절 |

---

## 3. 스터디 핵심 질문 6개와 답하는 층

우리가 매 회차 설계 학습지와 리뷰에서 반복한 **핵심 질문 6개**는 특정 도구 하나가 아니라 파이프라인의 각 층과 검증 장치가 나누어 답합니다(설계: [`labs/README.md`](../README.md), [`labs/session07/student-guide.md`](../session07/student-guide.md#9-개인-프로젝트-리뷰-질문지-핵심-6--67회차-3)).

| 핵심 질문 | 답하는 층과 장치 (코드·문서·설계) | 이 과정의 구체적 근거 (실측·코드) |
|---|---|---|
| **① 누가 어떤 판단에 쓰는 데이터인가요?**<br>**② 얼마나 빨리 반영돼야 하나요(신선도)?** | **업무 요구 정의 → 수집·변환 주기 결정**<br>(스트리밍 Bronze vs 배치 Silver·Gold) | 정산 담당자가 주문일별 순결제액을 확인함. 원천 변경→Bronze는 Flink 10초 체크포인트로 빠르게 남기고, Silver·Gold는 배치 주기(수업 예제 5분 스케줄 또는 30분 설계 가정)로 갱신 (코드·설계) |
| **③ 수정·삭제·중복 전달은 어떻게 처리하나요?** | **수집(`REPLICA IDENTITY FULL`) + Bronze(원문 보존) + Silver 현재 상태(버전 가드 `MERGE`)** | `op=d`의 `before` 값을 보존하고, Silver에서 `ROW_NUMBER(ORDER BY source_lsn DESC, kafka_offset DESC)`와 `WHEN MATCHED` 버전 가드로 중복·과거 `UPDATE` 역전을 거절하며 `is_deleted`를 남김 (코드·실측) |
| **④ 실패하면 어디서 다시 시작하나요?** | **층별 복구 지점 분리**<br>(slot LSN / Kafka offset / Flink checkpoint·savepoint / Bronze 재조회 / Gold `plan`·`progress`) | 각 층이 자기 앞단의 보존된 상태(WAL, Kafka 토픽, Flink 상태 파일, Iceberg 스냅샷·`plan`·`progress`)에서 이어 달리고 하류는 멱등하게 덮어씀 (코드·문서·실측) |
| **⑤ 맞다는 근거는 무엇인가요?** | **Silver 입력 검증 + dbt 모델 계약(5개) + 데이터 테스트(15개) + 양방향 `EXCEPT` 대조 + `verify.sh`** | 스키마는 교체 전 `contract`로 막고, 키 중복·참조 무결성·`net_paid = paid - refunded`는 테스트 15개(`PASS=20`)로, 증분 결과는 전체 계산과 `EXCEPT` 0건으로 검증 (코드·실측 2026-11-01·11-02·11-13) |
| **⑥ 왜 그 구조를 골랐고 무엇을 감수했나요?** | **설계 결정과 트레이드오프 기록**<br>(4절 설계 결정표) | 구현·복구 단순성을 위해 초기에는 전체 재조회·전체 교체·COW·배치를 고르고, 데이터가 커질 때 필요한 증분(`lesson06`)과 유지보수(`lesson07`) 비용을 분리해 검증함 (코드·설계) |

---

## 4. 설계 결정표: 고른 것 · 대안 · 감수한 비용

실무 설계 리뷰에서는 "무엇을 썼는가"보다 **"어떤 대안과 비교해 왜 이것을 골랐고, 그 결과 어떤 비용이나 한계를 감수했는가"**를 묻습니다. 저장소 코드와 각 회차 가이드에 근거가 있는 핵심 설계 결정 다섯 가지입니다(코드·문서·설계).

| 결정 주제 | 공통 파이프라인에서 고른 것 (코드) | 고려할 수 있는 대안 (문서·설계) | 고른 이유 (코드·문서·설계) | 감수한 비용과 깨지는 조건 (실측·문서·설계) |
|---|---|---|---|---|
| **① Bronze → Silver 읽기 범위** | 매번 `lake.bronze.<table>` **전체를 다시 읽어** PK별 최신 1건을 고른 뒤 Silver에 `MERGE` ([`scripts/pipeline.py`](../../scripts/pipeline.py)) | Flink 스트리밍 업서트 또는 Bronze 스냅샷 구간(watermark) 증분 읽기 | 진행 위치 상태 테이블 없이도 언제든 다시 돌리면 같은 Silver 상태가 만들어져 복구와 검증이 단순함 (코드·문서: [`docs/operations.md`](../../docs/operations.md)) | Bronze 이력이 쌓일수록 매 주기 전체 스캔 비용이 커짐 → 대용량에서는 스냅샷 범위 증분 읽기가 필요함 (문서·설계) |
| **② Silver core · Gold 갱신 방식** | 공통 파이프라인은 dbt `materialized: table` **전체 교체** ([`dbt_project.yml`](../../dbt/dbt_project.yml)) | 6회차 실험판([`labs/session06/lab.py`](../session06/lab.py))처럼 `create_changelog_view` + `plan`·`progress` 기반 **영향 키·날짜 증분 `MERGE`** (또는 dbt `incremental`) | 전체 교체는 삭제·과거 날짜 이동·중간 실패 후 재시도에서 유령 행이 남을 위험이 없어 초기 기준 구현으로 가장 안전함 (코드·설계: [5회차 가이드](../session05/student-guide.md)) | 전체 교체는 데이터가 커지면 시간·비용이 늘고, 반대로 증분 `MERGE`를 택하면 키 전파·빈 날짜 `DELETE`·`plan`/`progress` 관리·스냅샷 보존 하한선 복잡도를 감수해야 함 (실측·코드: [6회차 가이드](../session06/student-guide.md)) |
| **③ Iceberg 쓰기 모드** | **Copy-on-Write (COW)** 기본값 사용 (6회차 실험판도 `write.merge.mode='copy-on-write'` 명시, 코드) | Merge-on-Read (MOR, delete file 기록 후 추후 압축) | 조회 시 delete file을 병합할 필요가 없어 읽기 구조가 단순하고, `create_changelog_view`로 carry-over 행을 걸러 변경 추적이 명확함 (실측·코드·문서: [6회차 가이드](../session06/student-guide.md)) | `MERGE`할 때마다 변경된 행과 같은 파일에 있던 이웃 행(예: `s5`의 `o2`)까지 새 Parquet 파일로 다시 써지며 스냅샷·파일 정리가 필수임 (스터디 공통 환경 실측 2026-11-02·11-07) |
| **④ Silver · Gold 실행 엔진과 주기** | Bronze까지만 **Flink 스트리밍**이고, Silver·Gold는 **Spark + dbt 배치** 실행 (코드) | 원천부터 Gold까지 전 구간 Flink 스트리밍 조인·집계 | 원천 변경 유실 방지는 Bronze 스트리밍 적재로 확보하고, 복잡한 정산 규칙·조인·데이터 테스트 15개·모델 계약은 배치 SQL과 dbt로 관리해 상태 복구 부담을 줄임 (코드·설계: [3회차](../session03/student-guide.md)·[5회차 가이드](../session05/student-guide.md)) | Gold 지표 반영 지연이 배치 주기에 묶이므로 초 단위 실시간 서빙이 필요한 업무에는 맞지 않음 (설계) |
| **⑤ Bronze 테이블 분리 단위** | 2026-11-01부터 **Kafka 토픽마다 Bronze 테이블 1개**(`orders`, `payments`, `refunds`), Flink 잡은 `EXECUTE STATEMENT SET`으로 1개 유지 ([`flink/ingest.sql.template`](../../flink/ingest.sql.template)) | 세 토픽을 단일 테이블(`lake.bronze.cdc_events`) 하나에 모으기 (2026-11-01 이전 구조) | Silver 변환이 각 테이블만 읽으면 되고, offset이 토픽·파티션별 값이라는 경계가 명확해짐 (코드·문서: [4회차 가이드](../session04/student-guide.md), [`docs/operations.md`](../../docs/operations.md)) | 한 Flink 잡이 세 테이블에 동시에 커밋하므로, 유휴 시간대 빈 커밋과 `metadata.json`도 테이블 수(3배)만큼 함께 늘어남 (스터디 공통 환경 실측 2026-11-05) |

---

## 5. Airflow: 스케줄러와 작업의 책임 분담

6회차 끝에서 살펴본 것처럼, 오케스트레이션 도구(Airflow)를 붙일 때 가장 중요한 원칙은 **"스케줄러가 책임지는 것"**과 **"파이프라인 작업 자체가 책임지는 것"**을 섞지 않는 것입니다(문서·설계: [6회차 가이드](../session06/student-guide.md#airflow-30분-주기-스케줄과-멱등성-코드문서설계)).

### 저장소의 `lakehouse_refresh` DAG가 하는 일 (코드)

[`airflow/dags/lakehouse_refresh.py`](../../airflow/dags/lakehouse_refresh.py)는 단일 노드 실습 환경에서 5분마다 내부 `runner` 컨테이너의 HTTP API(`http://runner:8080/runs`)를 호출해 Silver 갱신과 `dbt build`를 실행합니다(코드, [`docs/operations.md`](../../docs/operations.md#후속-회차--airflow)).

```python
with DAG('lakehouse_refresh', start_date=datetime(2026,1,1,tzinfo=timezone.utc),
         schedule='*/5 * * * *', catchup=False, max_active_runs=1,
         is_paused_upon_creation=True, default_args={'retries':0},
         dagrun_timeout=timedelta(minutes=25), tags=['cdc','iceberg','dbt']) as dag:
    PythonOperator(task_id='silver_then_dbt_build', python_callable=refresh)
```

- **보안과 격리 경계**: Airflow 컨테이너에 Docker 소켓(`/var/run/docker.sock`)을 넘기지 않고, Compose 내부 네트워크의 `runner` API에 고정된 작업만 요청합니다(문서: [`docs/operations.md`](../../docs/operations.md#후속-회차--airflow)).

### 스케줄러와 파이프라인 작업의 역할 분담 (코드·문서·설계)

| 구분 | 설정 또는 구현 | 무엇을 보장하나요? | 근거 |
|---|---|---|---|
| **Airflow (스케줄러)** | `schedule='*/5 * * * *'` (또는 Gold 증분 설계 시 `*/30 * * * *`) | 정해진 주기에 맞춰 작업을 깨움 | 코드([`lakehouse_refresh.py`](../../airflow/dags/lakehouse_refresh.py)), 설계([6회차 가이드](../session06/student-guide.md)) |
| | `max_active_runs=1` (+ `pipeline.py`의 `flock` 파일 잠금) | 이전 실행이 끝나기 전에 다음 주기가 겹쳐 두 작업이 동시에 같은 테이블·`progress`를 덮어쓰는 일을 막음 | 코드([`lakehouse_refresh.py`](../../airflow/dags/lakehouse_refresh.py), [`scripts/pipeline.py`](../../scripts/pipeline.py) 63~65행) |
| | `catchup=False` | 파이프라인이 현재 상태 전체(또는 `progress`~`main` 스냅샷 구간)를 한 번에 처리하므로, 멈춰 있던 과거 시간대마다 불필요한 실행을 수십 번 쌓지 않음 | 코드·문서([Airflow DAG Runs](https://airflow.apache.org/docs/apache-airflow/2.10.5/core-concepts/dag-run.html)) |
| | `is_paused_upon_creation=True` | 초기 환경 구축과 수동 검증이 끝나기 전에 스케줄이 혼자 돌지 않도록 보호 | 코드·문서([`docs/operations.md`](../../docs/operations.md)) |
| **파이프라인 작업 (Silver · dbt · 증분)** | 버전 가드 `MERGE`, 전체 재합산, `plan` 선저장 · `progress` 후갱신 | 스케줄러가 실패한 작업을 다시 호출했을 때 **중복 가산이나 날짜 누락 없이 같은 결과(멱등성)**를 내는 것은 작업 내부 SQL과 상태 순서의 책임임 | 코드·실측([4회차](../session04/student-guide.md)·[6회차 가이드](../session06/student-guide.md)), 문서([Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/2.10.5/best-practices.html)) |

- Airflow 실행 명령과 UI 접속 방법은 [`docs/operations.md`의 Airflow 절](../../docs/operations.md#후속-회차--airflow)과 [`docs/follow-along.md` 단계 7](../../docs/follow-along.md#단계-7-선택-airflow-화면과-스케줄-설계-확인-6회차)을 참고합니다(문서).

---

## 6. 장애 대응 사례: 조용히 쌓이다 한꺼번에 멈춘 날 (스터디 공통 환경 실측)

7회차에서 원인을 분석했던 스터디 공통 환경의 디스크 포화 사고가 실제로 어떤 순서로 번졌고, 2026년 11월 13일에 어떤 절차로 복구·검증했는지 전체 기록입니다(스터디 공통 환경 실측 2026-11-05·2026-11-13, 모든 시각은 KST 기준).

### ① 사고 타임라인 (스터디 공통 환경 실측 2026-11-01 ~ 2026-11-13)

| 시각 (KST) | 일어난 일 | 관측된 상태와 숫자 (스터디 공통 환경 실측) |
|---|---|---|
| **11/1(일) 16:51** | 토픽별 Bronze 테이블(`orders`, `payments`, `refunds`) 전환 후 Flink `cdc-bronze` 작업 시작 | 첫 스냅샷 생성(11/1 07:51 UTC). 이후 약 3.4일 동안 원천 변경이 없어도 **약 100초마다 빈 커밋** 반복 |
| **11/1 ~ 11/5** | Bronze 세 테이블에 스냅샷과 `metadata.json` 누적 | 테이블마다 **snapshot 2,952개**, **`metadata.json` 2,953개**(파일당 8 KB → **2.4 MB**) 누적. 실제 데이터는 **27·28·63행**(파일 1개씩, 1 MB 미만) |
| **11/5(목) 01:39** | 마지막으로 성공한 Iceberg 커밋 | 스냅샷 summary의 `flink.max-committed-checkpoint-id = 29515` |
| **11/5 01:41** | 디스크가 가득 차 Flink의 Iceberg 커밋 실패 | `S3Exception ... cause(no space left on device)` (Status 500), Flink 작업 `RUNNING` → `RESTARTING` |
| **11/5 01:56** | 마지막 Flink 체크포인트 기록 | `chk-29615` (Iceberg 커밋이 실패한 뒤에도 Flink 체크포인트는 약 15분 더 기록됨) |
| **11/5 02:32** | Kafka 브로커 비정상 종료 (`exit 1`) | `replication-offset-checkpoint` 쓰기 실패(`No space left on device`). 컨테이너 목록에는 한동안 `"Up (healthy)"`로 보였으나 실제 프로세스는 `exited` 상태 |
| **11/5 02:32 이후** | Debezium Connect 재연결 로그 폭주 | 죽은 Kafka를 찾으며 `"Rebootstrapping"` 로그를 **분당 약 77 MB씩 총 2.1 GB** 기록 |
| **11/5 19:10** | 수업 50분 전 표면 증상 발견 및 응급 조치 | Explorer에서 `"Spark Thrift 실패"`, OpenMetadata 검색 인덱스 읽기 전용 잠금 발견. 미사용 이미지 7.7 GB 삭제(디스크 100% → 82%) 및 Connect 정지로 임시 우회했으나, 근본 원인(Bronze 메타데이터)을 두어 디스크가 다시 94%까지 차오름(11/8 검색 인덱스 재구축 멈춤) |
| **11/13(금) 16:21~16:36** | 근본 복구 및 종단 검증 완료 | 약 15분 동안 Bronze 정리 → Kafka·Connect·Flink 복원 → `verify.sh` 통과 → 고아 파일 삭제 완료 |

### ② 진단 순서: 증상에서 근본 원인까지 거슬러 가기 (실측 2026-11-13 16:21 KST)

장애가 났을 때 가장 먼저 눈에 띄는 에러 메시지(Spark 접속 실패, Connect 로그 2.1 GB)는 원인이 아니라 **결과**였습니다. 복구 직전(11/13 16:21 KST) 스터디 공통 환경을 진단한 순서와 확인 결과입니다(스터디 공통 환경 실측 2026-11-13).

1. **표면 증상 확인**: Lakehouse Explorer에서 Spark Thrift 세션이 열리지 않음 → Spark가 임시 세션 디렉터리를 디스크에 쓰지 못함(디스크 94%, 여유 2.6 GB).
2. **어느 디렉터리가 디스크를 채웠는지 확인 (`du`)**: 객체 저장소의 `warehouse`가 **13.75 GB**를 차지하고 있었고, 그중 Bronze 세 테이블(`orders`, `payments`, `refunds`)의 `metadata` 폴더가 각각 **3.60 GB, 3.61 GB, 3.60 GB**인 반면 `data` 폴더는 모두 **1 MB 미만**이었습니다(옛 `bronze.cdc_events` 메타데이터는 1.88 GB).
3. **왜 메타데이터가 불어났는지 확인**: 10초 체크포인트 × `flink.max-continuous-empty-commits`(기본 10) = 약 100초마다 빈 커밋이 생겼고, `write.metadata.delete-after-commit.enabled`(기본 `false`) 때문에 추적 한도(101개) 밖으로 밀려난 옛 `metadata.json` 2,852~2,854개가 지워지지 않고 남아 있었습니다.
4. **상류부터 하류까지 멈춘 상태와 유실 여부 확인**:
   - **원천 PostgreSQL**: replication slot `cdc_lab` 비활성(`active=false`), 아직 전달되지 않은 WAL은 **760 bytes**뿐이며 원천 행 수는 `orders` 27, `payments` 27, `refunds` 7행이었습니다.
   - **Kafka · Connect**: 두 컨테이너 모두 `exited` 상태였습니다.
   - **Flink**: 실행 중인 작업 0개. 상태 파일(`.runtime/flink-state.json`)에는 `job_id`만 있고 `savepoint`가 없었습니다. 저장소의 [`scripts/flink_state.py`](../../scripts/flink_state.py) `prepare`는 예고 없이 죽은 작업을 조용히 처음부터 재생하지 못하도록 `"Previous job has no saved stop point. Recover explicitly"` 오류를 내며 제출을 거절합니다(코드·실측).
   - **Bronze**: `orders` 27행, `payments` 28행, `refunds` 63행(마지막 커밋 11/5 01:39 KST).

### ③ 복구 순서와 그 이유 (실측 2026-11-13 16:21~16:36 KST, 약 15분)

아무 컨테이너나 먼저 재시작하거나 파일을 무작정 지우지 않고, 아래 6단계 순서를 지켰습니다(스터디 공통 환경 실측 2026-11-13).

```text
[0. 쓰기 멈춤 확인 · 기준 기록]
       ↓
[1. Bronze 속성 변경 + expire_snapshots(retain_last => 1)]  ← 최신 스냅샷 1개 보존 (고아 파일은 아직 안 지움)
       ↓
[2. Kafka 재기동] → [3. Connect 재기동] → [4. Flink 마지막 checkpoint(chk-29615)에서 명시적 복원]
       ↓
[5. ./scripts/verify.sh 종단 검증 (원천 → Bronze → Silver → Gold)]
       ↓
[6. 검증 통과 뒤 remove_orphan_files (dry_run 확인 후 삭제)]
```

- **단계 0 — 쓰기 멈춤 확인과 사전 기록**: Flink 실행 작업이 0개이고 Kafka·Connect가 멈춘 것을 먼저 확인한 뒤, 정리 전후 데이터 불변성을 대조하기 위해 Bronze 세 테이블의 행 수(`27, 28, 63`)와 내용 해시를 기록했습니다(실측).
- **단계 1 — Bronze 테이블 속성 설정과 스냅샷 만료 (16:26~16:27 KST, 테이블당 8~9초)**:
  - 재발 방지를 위해 세 Bronze 테이블에 `write.metadata.delete-after-commit.enabled=true`, `write.metadata.previous-versions-max=20`, `flink.max-continuous-empty-commits=360`(빈 커밋 간격을 약 100초 → 약 1시간으로 완화)을 설정했습니다(실측).
  - 이어서 `expire_snapshots(retain_last => 1)`을 실행해 테이블마다 만료된 manifest list **2,951개**(`deleted_manifest_lists_count = 2951`, 데이터 파일 삭제 0개)를 지웠습니다.
  - **왜 최신 스냅샷 1개는 남기고, 고아 파일 삭제는 미뤘나요?**: Flink가 마지막으로 커밋한 체크포인트 번호(`flink.max-committed-checkpoint-id = 29515`)가 최신 스냅샷 summary에 적혀 있어 Flink 상태 복원에 필요하며(문서: [Iceberg Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/)), Flink가 복원을 마치기 전에 고아 파일을 지우면 복구 지점이 참조하는 파일을 건드릴 위험이 있기 때문입니다(실측·문서·설계).
- **단계 2 — Kafka 재기동 (16:29~16:30 KST)**:
  - 3초 만에 `healthy`가 되었고 토픽 8개와 파티션 85개가 오류 없이 올라왔습니다.
  - 토픽 끝 오프셋(`orders` 27, `payments` 28, `refunds` 80)과 Flink 소비 그룹(`flink-cdc-bronze`) 지연 **0**을 확인했습니다. Bronze 행 수(`27, 28, 63` — `refunds`는 tombstone 17건 제외)와 정확히 맞아 **유실이 없었음**을 확인했습니다(실측).
- **단계 3 — Debezium Connect 재기동 (16:30~16:31 KST)**:
  - 12초 만에 커넥터와 태스크가 `RUNNING`이 되었고, 원천 PostgreSQL의 replication slot `cdc_lab`이 `active=true`로 돌아왔습니다(실측).
- **단계 4 — Flink를 마지막 체크포인트(`chk-29615`)에서 명시적으로 복원 (16:31~16:32 KST)**:
  - `.runtime/flink-state.json`의 `savepoint` 필드에 남아 있던 마지막 체크포인트 경로(`.../chk-29615`)를 직접 지정해 `flink_state.py prepare`가 `SET 'execution.savepoint.path' = '.../chk-29615';`를 붙여 제출하도록 했습니다(실측·코드: [`scripts/flink_state.py`](../../scripts/flink_state.py)).
  - 제출 45초 뒤 체크포인트 `restored = 1`, `completed = 5`, `failed = 0`, 소비 그룹 지연 0을 확인했습니다(실측).
- **단계 5 — 종단 검증 `./scripts/verify.sh` (16:32~16:34 KST)**:
  - 컨테이너가 뜬 것만으로 복구 완료로 보지 않고, 원천 DB에 새 주문·결제·환불 변경을 넣어 Gold까지 대조했습니다(아래 ⑤절 표 참고, 실측).
- **단계 6 — 고아 파일 삭제 (16:35 KST, 검증 통과 뒤)**:
  - Flink 복원과 데이터 검증이 모두 끝난 뒤, `older_than`을 24시간보다 이전으로 두고 `prefix_listing => true`, `dry_run => true`로 삭제 대상을 먼저 확인했습니다(삭제 대상이 옛 `metadata.json`과 실패한 커밋이 남긴 `snap-*.avro`뿐임을 확인, 실측).
  - 실제 삭제를 실행해 테이블당 **2,863~2,865개**(`metadata.json` 2,852~2,854개 + `snap-*.avro` 11개)를 지웠고, 삭제 뒤에도 Flink 작업은 계속 `RUNNING`을 유지했습니다(실측).

### ④ 복구 전후 실측 숫자 비교 (스터디 공통 환경 실측 2026-11-13)

| 측정 항목 | 복구 전 (11/13 16:21 KST) | 1단계 `expire_snapshots` 직후 | 6단계 `remove_orphan_files` 직후 |
|---|---|---|---|
| **Bronze 테이블당 `snapshots` 수** | **2,952개** | **1개** (manifest list 2,951개 삭제) | **1개** (이후 정상 커밋 진행) |
| **Bronze `.metadata_log_entries` 행 수** | **101행** | **21행** (`previous-versions-max=20` 반영) | **21행** |
| **최신 `metadata.json` 1개 파일 크기** | **약 2.4 MB** | **약 12 KB** | **약 12 KB** |
| **테이블당 저장소 `metadata.json` 파일 수** | **2,953개** | 약 2,873~2,875개 (고아 파일 삭제 전) | **21개** (고아 `metadata.json` 2,852~2,854개 삭제) |
| **Bronze 테이블별 `metadata` 폴더 크기** | **3.60 / 3.61 / 3.60 GB** | — | **46 / 46 / 37 MB** |
| **전체 `warehouse` 사용량** | **13.75 GB** | — | **3.08 GB** (옛 `cdc_events` 1.88 GB는 미삭제 유지) |
| **Bronze 행 수 (`orders` / `payments` / `refunds`)** | **27 / 28 / 63행** | **27 / 28 / 63행** (해시 동일) | 검증 케이스 행 추가 후 정상 유지 |

- **운영 관찰 포인트 ("지웠는데 왜 디스크 사용률이 바로 안 줄었나요?", 실측)**: 객체 저장소에서 약 10 GB의 파일을 지운 직후에도 호스트 디스크 사용률은 92%였습니다. 객체 저장소가 삭제된 객체를 내부 휴지통 경로에 잠시 보관한 뒤 비우기 때문이며, 약 5분 뒤 다시 재 보니 66%(여유 13.4 GB)로 내려가 있었습니다. 실무에서도 Iceberg 정리 직후 호스트 파일시스템 여유 공간이 실제로 회수되었는지까지 확인해야 합니다(스터디 공통 환경 실측 2026-11-13).

### ⑤ 복구 검증 결과: 컨테이너가 떴다고 끝이 아니다 (스터디 공통 환경 실측 2026-11-13 16:32~16:34 KST)

복구 직후 실행한 `./scripts/verify.sh`의 6개 검증 케이스 결과입니다(스터디 공통 환경 실측 2026-11-13).

| 검증 단계 (`case`) | 시험한 상황 | 기대 `net_paid` | 실제 `net_paid` | 통과 여부 (실측 2026-11-13) |
|---|---|---:|---:|---|
| `insert` | 주문 + 결제 100,000원 생성 | 100,000 | 100,000 | 일치 |
| `refund_insert` | 환불 20,000원 생성 (`op=c`) | 80,000 | 80,000 | 일치 |
| `duplicate_replay` | 동일 이벤트 중복 적재 후 Silver·Gold 재실행 | 80,000 | 80,000 | 일치 |
| `refund_update` | 환불 금액 30,000원으로 정정 (`op=u`) | 70,000 | 70,000 | 일치 |
| `refund_delete` | 환불 물리 삭제·철회 (`op=d`) | 100,000 | 100,000 | 일치 |
| `old_update_after_delete` | 삭제 뒤에 과거 LSN의 `UPDATE` 재전달 | 100,000 | 100,000 | 일치 (가드 거절) |

- **지연 및 테스트 결과**: 원천 변경 → Bronze 첫 관측까지 **4.9~12.8초**, 원천 변경 → Gold 검증 완료까지 **16.2~25.1초**가 걸렸으며, `dbt build` 20단계(`PASS=20`)와 마지막 출력 `PASS: artifacts/runs/verification-20261113T073218Z.json`을 확인했습니다(스터디 공통 환경 실측 2026-11-13).
- **왜 데이터 유실이 없었나요? (구조적 이유, 코드·문서)**:
  1. **Debezium**: 원천 PostgreSQL의 replication slot(`cdc_lab`)이 마지막으로 확인된 WAL 위치(`confirmed_flush_lsn`)를 잡고 있었고(문서: [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)),
  2. **Kafka**: 브로커가 디스크에 이미 받은 토픽 로그를 보관하고 있었으며,
  3. **Flink**: 체크포인트(`chk-29615`)에 Kafka 소비 오프셋과 Iceberg 커밋 상태를 기록해 두었고(문서: [Flink Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/checkpointing/)),
  4. **Silver · Gold**: Bronze 전체를 다시 읽고 `source_lsn`·`kafka_offset` 버전 가드로 `MERGE`한 뒤 `dbt build`로 전체 교체하므로, 같은 구간을 다시 처리해도 결과가 흔들리지 않았기 때문입니다(코드: [`scripts/pipeline.py`](../../scripts/pipeline.py)).

### ⑥ 재발 방지 기준 (실측·설계)

| 영역 | 적용하거나 설계할 조치 | 구분 |
|---|---|---|
| **테이블 속성** | Bronze 세 테이블에 `write.metadata.delete-after-commit.enabled=true`, `write.metadata.previous-versions-max=20`, `flink.max-continuous-empty-commits=360` 적용 완료 (새 환경을 만드는 [`flink/ingest.sql.template`](../../flink/ingest.sql.template)에도 같은 속성을 넣도록 설계) | 실측(2026-11-13) · 설계 |
| **정기 유지보수 주기** | 7회차 순서(`compact` → `expire` → `orphans` → `manifests`)를 주기 작업으로 등록하되, Flink 적재 테이블은 최신 스냅샷을 남기고 고아 파일은 24시간보다 오래된 것만 삭제 | 설계 ([7회차 가이드](../session07/student-guide.md#7-운영-기준-체크리스트와-flink-테이블-주의점)) |
| **핵심 감시 지표** | ① 호스트 디스크 사용률(80% 경고) ② 테이블별 snapshot 수·`metadata` 폴더 크기 ③ 마지막 성공 커밋 시각(멈춤 감지) ④ Kafka 소비 지연(lag) ⑤ PostgreSQL replication slot의 밀린 WAL 크기 ⑥ 컨테이너의 실제 프로세스 상태(목록 표시만 믿지 않고 `inspect`·`exec`로 확인) | 실측 · 설계 |
| **유휴 시간 운영** | 실습하지 않는 시간에는 반드시 `./scripts/stop.sh`로 Flink savepoint를 남기고 컨테이너를 중지 | 코드([`scripts/stop.sh`](../../scripts/stop.sh)) · 문서([`docs/operations.md`](../../docs/operations.md)) |

---

## 7. 층별 재처리 시작점: 어디서 멈추면 어디서 다시 시작하나요?

장애가 났을 때 전체 환경을 초기화(`down -v`)하지 않고, 멈춘 층만 짚어 이어 달리는 기준표입니다(코드·문서: [`docs/operations.md`](../../docs/operations.md#후속-회차--복구-실험과-경계), [`scripts/recovery.sh`](../../scripts/recovery.sh), [`scripts/flink_state.py`](../../scripts/flink_state.py), [`scripts/stop.sh`](../../scripts/stop.sh)).

| 멈춘 구간 | 무엇이 진행 위치를 기억하나요? (코드·문서) | 어떻게 다시 시작하나요? (코드·실측) | 복구 한계와 깨지는 조건 (문서·설계) |
|---|---|---|---|
| **원천 PostgreSQL → Kafka**<br>(Debezium Connect 중단) | PostgreSQL replication slot(`cdc_lab`)의 `confirmed_flush_lsn`과 Connect 내부 offset 토픽 (문서·코드) | Connect 컨테이너·태스크 재기동 시 마지막 LSN부터 이어서 WAL을 읽음 (실측 2026-11-13) | Connect가 오래 멈춘 동안 원천 DB에 변경이 많으면 WAL이 지워지지 못해 **원천 DB 디스크가 가득 찰 수 있음** (문서: [Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)) |
| **Kafka → Bronze**<br>(Flink 워커 장애 또는 계획된 종료) | Flink checkpoint / savepoint의 Kafka 소스 오프셋과 Iceberg 커밋 체크포인트 ID (코드·문서) | • **계획된 종료**: `./scripts/stop.sh`(savepoint 생성) → `./scripts/bootstrap.sh --no-build`<br>• **TaskManager 일시 중단**: JobManager가 최근 checkpoint에서 자동 복구 (`./scripts/recovery.sh` 실험)<br>• **JobManager까지 비정상 종료**: 마지막 정상 checkpoint/savepoint 경로를 확인해 명시적 복원 (`flink_state.py`가 무상태 자동 재생 차단) (코드·실측) | 중단 기간이 **Kafka 토픽 보관 기간(기본 7일)**을 넘기거나, 참조해야 할 체크포인트·스냅샷 파일이 삭제되면 이어 읽을 수 없음 (문서: [`docs/operations.md`](../../docs/operations.md)) |
| **Bronze → Silver 현재 상태**<br>(Spark `pipeline.py` 실패) | 별도 진행 위치 없음 — Bronze 테이블 자체가 전체 원문 이력을 보관 (코드) | `./scripts/lab.sh exec -T runner python scripts/pipeline.py`를 그냥 다시 실행 (`ROW_NUMBER` + LSN·offset 가드 `MERGE`가 멱등하게 최신 상태만 반영) (코드·실측) | Bronze 테이블이 손상되었거나, 데이터 규모가 커져 전체 스캔 시간이 목표 주기를 넘기는 경우 (설계) |
| **Silver → Silver core · Gold**<br>(dbt 빌드 또는 6회차 증분 실패) | • **5회차 기본(`table`)**: Silver 현재 상태 테이블<br>• **6회차 증분(`lesson06`)**: `progress`(마지막 성공 스냅샷 ID)와 `plan`(쓰기 전 고정한 키·날짜 계획) (코드) | • **5회차 기본**: `dbt build` 재실행(테이블 전체 교체)<br>• **6회차 증분**: 같은 범위 재시도 시 저장된 `plan`을 재사용(`reused: true`)해 두 마트를 마저 맞춘 뒤 `progress` 갱신 (코드·실측 2026-11-02) | 증분 모드에서 장기 장애로 `expire_snapshots`가 `progress` 스냅샷까지 지웠다면(`progress-trap`), **전체 재계산 1회 실행 후 최신 스냅샷으로 `progress`를 재설정**해야 함 (실측·설계: [7회차 가이드](../session07/student-guide.md#6-6회차-연결-증분-시작점progress을-지우면-생기는-일-progress-trap)) |

> **스터디 공통 환경 실측(2026-11-13)**: 이번 중단은 여드레로 Kafka 기본 보관 기간(7일)보다 길었습니다. 그래도 유실이 없었던 것은 Flink가 멈추기 전에 토픽 끝까지 읽어 둔 상태(lag 0)였고, 멈춘 동안 원천 변경이 없었기 때문입니다. 복구 약 20분 뒤 토픽의 가장 오래된 오프셋은 27·28·80(끝 28·29·86)이었습니다. 11월 초 이벤트는 Kafka에서 지워졌고 지금은 Bronze에만 남아 있습니다. 읽지 못한 메시지가 남은 채 보관 기간을 넘겼다면 checkpoint가 있어도 이어 읽을 수 없습니다.

---

## 8. 집에서 해 볼 것: 복구 실험 · 계획된 중지 · 내 환경 점검

모든 명령은 **개인 컴퓨터의 저장소 루트**에서 실행합니다. 1~7회차 전체 과정을 처음부터 순서대로 따라 해 보려면 **[1~7회차 통합 따라 하기 (`docs/follow-along.md`)](../../docs/follow-along.md)**를 참고합니다(문서).

### ① `./scripts/recovery.sh`: TaskManager 중지 중 변경과 자동 복구 실험 (코드·문서)

[`scripts/recovery.sh`](../../scripts/recovery.sh)와 [`scripts/recovery.py`](../../scripts/recovery.py)는 JobManager를 유지한 채 실제 처리를 담당하는 **TaskManager 컨테이너만 멈추고**, 그사이에 원천 DB의 결제 금액을 `50,000`원에서 `70,000`원으로 바꾼 뒤 TaskManager를 다시 살려 Flink가 체크포인트에서 복구하고 Gold(`mart_order_settlement`)까지 `70,000`원으로 반영되는지 검증합니다(코드: [`scripts/recovery.sh`](../../scripts/recovery.sh), [`scripts/recovery.py`](../../scripts/recovery.py), 문서: [`docs/operations.md`](../../docs/operations.md#후속-회차--복구-실험과-경계)).

```bash
# 필수 컨테이너가 실행 중인 상태에서 실행
./scripts/recovery.sh
```

- **내부 실행 4단계 (코드: [`scripts/recovery.py`](../../scripts/recovery.py))**:
  1. `prepare`: 새 주문(`REC-<시각>`)과 결제(`50,000`원)를 원천에 넣고 Bronze 적재 및 `build()` 완료 후 현재 Flink 체크포인트 복원 횟수(`restored`)를 `.runtime/recovery.json`에 기록합니다.
  2. `stop taskmanager`: `./scripts/lab.sh stop taskmanager`로 워커를 멈춥니다(`trap`이 걸려 있어 스크립트가 중간에 끝나도 `taskmanager`를 다시 켭니다).
  3. `mutate`: 워커가 멈춘 상태에서 원천 결제 금액을 `70,000`원으로 `UPDATE`합니다.
  4. `start taskmanager` + `check`: 워커를 다시 켜고 `u`(`70,000`원) 이벤트가 Bronze에 들어온 뒤 Silver·Gold를 빌드해 `net_paid == 70000`, `restored_after > restored_before`, 중복 오프셋 `0`건을 확인하고 `PASS: artifacts/runs/recovery-<시각>.json`을 출력합니다.
- **주의점**: 복구 실험 도중에는 스냅샷 만료(`expire_snapshots`)나 고아 파일 삭제(`remove_orphan_files`)를 동시에 실행하지 않습니다(문서: [`labs/README.md`](../README.md)).

### ② `./scripts/stop.sh` → 재기동: 계획된 종료와 savepoint 복원 (코드·문서)

실습을 마칠 때는 컨테이너를 강제로 끄지 않고 [`scripts/stop.sh`](../../scripts/stop.sh)로 Flink savepoint를 남기고 종료합니다(코드: [`scripts/stop.sh`](../../scripts/stop.sh), [`scripts/flink_state.py`](../../scripts/flink_state.py), 문서: [`docs/operations.md`](../../docs/operations.md#시작중지)).

```bash
# 1) Airflow 중지 → Flink savepoint 생성(.runtime/flink-state.json 기록) → 컨테이너 중지
./scripts/stop.sh

# 2) 다음에 다시 시작할 때 (로컬 이미지 그대로 사용, savepoint에서 이어서 복원)
./scripts/bootstrap.sh --no-build
```

### ③ 개인 환경을 오래 켜 둔 사람을 위한 점검 SQL (실측·코드·문서)

개인 Docker 환경을 며칠 동안 켜 두었다면, 6절의 스터디 공통 환경 사고처럼 Bronze 테이블에 빈 스냅샷과 `metadata.json`이 쌓였는지 아래 조회 SQL로 먼저 점검합니다(코드·설계).

```bash
# Bronze 세 테이블의 현재 스냅샷 수와 메타데이터 이력 수 확인 (조회만 수행, 변경 없음)
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql "
SELECT 'orders' AS tbl,
       (SELECT count(*) FROM lake.bronze.orders.snapshots) AS snapshots,
       (SELECT count(*) FROM lake.bronze.orders.metadata_log_entries) AS metadata_log
UNION ALL
SELECT 'payments',
       (SELECT count(*) FROM lake.bronze.payments.snapshots),
       (SELECT count(*) FROM lake.bronze.payments.metadata_log_entries)
UNION ALL
SELECT 'refunds',
       (SELECT count(*) FROM lake.bronze.refunds.snapshots),
       (SELECT count(*) FROM lake.bronze.refunds.metadata_log_entries)
"
```

- **점검 기준과 주의사항 (문서·설계)**:
  - 위 조회에서 `snapshots`가 수백~수천 개로 불어나 있다면, **실제 Bronze 테이블에 삭제 프로시저를 무작정 실행하지 마세요.**
  - 실행 중인 Flink 잡이 참조하는 최신 스냅샷을 지우거나 24시간 이내 파일을 지우면 복구 지점이 깨질 수 있습니다. 반드시 [7회차 학생 가이드 7절(운영 기준 체크리스트와 Flink 테이블 주의점)](../session07/student-guide.md#7-운영-기준-체크리스트와-flink-테이블-주의점)을 먼저 읽고, 고아 파일은 `dry_run => true`와 `prefix_listing => true`, `older_than`(최소 24시간 이전) 조건으로 목록부터 확인합니다(문서·설계).

---

## 9. 내 프로젝트 장애 한 줄 양식과 최종 운영 점검표

### ① 활동: 내 프로젝트 장애 시나리오 한 줄 쓰기 (설계)

내가 설계한 도메인 파이프라인에서 가장 일어날 법한 장애 하나를 골라 아래 네 칸을 채워 봅니다(설계).

| 항목 | 작성할 내용 | 공통 파이프라인 예시 (참고) |
|---|---|---|
| **① 겉으로 보이는 증상** | 사용자나 운영자가 처음 발견하는 현상 | 정산 대시보드의 어제·오늘 숫자가 안 바뀌거나 Spark 쿼리가 실패함 |
| **② 원인 후보 (상류→하류)** | 증상을 만들 수 있는 바닥 원인 1~2가지 | ① Flink 빈 커밋·메타데이터 누적으로 디스크 포화 ② 과거 날짜 정정 중 두 Gold 마트 사이에서 실패 |
| **③ 재시작 지점과 절차** | 데이터를 처음부터 다 지우지 않고 어디서 이어 달릴지 | ① 공간 확보 후 마지막 체크포인트에서 Flink 복원 ② 저장된 `plan`을 재사용해 Gold `MERGE` 재시도 후 `progress` 갱신 |
| **④ 복구 완료 검증 방법** | "컨테이너가 떴다" 말고 무엇을 대조해 맞다고 할지 | 새 검증 이벤트를 원천에 넣고 Gold 기대값·`dbt test`·전체 재계산과의 `EXCEPT` 0건을 확인 |

### ② 최종 운영 점검표 (문서·설계)

프로젝트를 마무리하기 전에 다른 사람(또는 한 달 뒤의 나)이 내 저장소만 보고 실행·검증·중지할 수 있는지 아래 다섯 항목으로 점검합니다(문서: [`labs/README.md`](../README.md) 8회차 5번, [`docs/operations.md`](../../docs/operations.md)).

| 번호 | 점검 항목 | 확인 기준 (코드·문서·설계) | 확인 |
|---|---|---|---|
| **1** | **README만으로 시작·조회·중지가 가능한가?** | 필요한 환경 조건, 최초 기동 명령, 핵심 결과 조회 SQL, 안전한 중지 명령(`stop.sh`, `down -v` 주의)이 빠짐없이 적혀 있는가 | `[ ]` |
| **2** | **비밀번호나 개인 로컬 경로가 코드에 남아 있지 않은가?** | `.env`나 Airflow 초기 비밀번호 파일 등을 Git에 올리지 않았고, 특정 개인 컴퓨터의 절대 경로가 하드코딩되어 있지 않은가 | `[ ]` |
| **3** | **같은 입력을 다시 실행해도 결과가 같은가(멱등성)?** | Silver `MERGE`나 Gold 갱신(전체 교체 또는 `plan`·`progress` 증분)을 두 번 연속 실행했을 때 행 수와 합계가 변하지 않는가 | `[ ]` |
| **4** | **맞다는 근거(계약·테스트·대조)가 실행 결과로 남는가?** | 모델 계약(`contract`), 데이터 테스트(키 유일성·참조 무결성·금액 정합성), 또는 전체 계산과의 `EXCEPT` 비교 결과를 재현할 수 있는가 | `[ ]` |
| **5** | **멈췄을 때 이어 달리는 지점과 보관 주기가 정해져 있는가?** | 각 층의 복구 지점(slot·offset·checkpoint·`progress`)과 스냅샷·메타데이터 정리 기준(`delete-after-commit`, `retain_last`)을 설명할 수 있는가 | `[ ]` |

---

## 10. 8회를 마치며 및 발표 안내

### 8회 동안 남긴 세 가지 (설계)

1. **도구 이름보다 층별 책임과 경계**: 왜 원문을 고치지 않는 Bronze가 필요한지, 왜 현재 상태(Silver)와 공통 fact·dim(Silver core), 소비자 지표(Gold)를 나누는지 스스로 기준을 세웠습니다.
2. **정답 외우기보다 트레이드오프 설명하기**: 전체 교체 대 증분 `MERGE`, COW 대 MOR, 스트리밍 대 배치 중에서 내 데이터의 크기와 신선도 목표에 맞는 방법을 고르고 감수한 비용을 말할 수 있게 되었습니다.
3. **정상 실행만큼 중요한 유지보수와 장애 복구**: 작은 커밋이 만드는 파일·메타데이터 누적을 치우는 순서와, 장애가 났을 때 상류부터 복원하고 종단 데이터로 검증하는 절차를 확인했습니다.

### 프로젝트 발표 안내

- **일정 및 형식**: 발표는 **별도 날짜**에 진행하며, 발표 형식과 제출 기한은 **별도 공지**를 따릅니다(문서: [`labs/README.md`](../README.md)). 수업 중 별도 제출 절차는 없습니다.
- **발표에서 나눌 내용**: 얼마나 많은 도구를 붙였는지보다 **① 내 도메인의 핵심 질문 6개에 대한 답 ② 가장 고민한 설계 결정과 감수한 트레이드오프 ③ 직접 겪은 오류나 검증·복구 과정**을 중심으로 나눕니다(설계).
