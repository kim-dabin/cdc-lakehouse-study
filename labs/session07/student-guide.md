# 7회차 학생 가이드: 작은 커밋이 쌓이면 — Iceberg 유지보수와 개인 프로젝트 리뷰

6회차에서는 Silver 현재 상태의 스냅샷 구간 변경(`create_changelog_view`)을 읽어 영향받은 주문 키와 날짜만 골라 Gold를 증분 갱신하고, 쓰기 전 `plan` 저장과 완료 후 `progress` 갱신으로 재시도 안전성(멱등성)을 확보했습니다(코드: [`labs/session06/lab.py`](../session06/lab.py)).

하지만 파이프라인이 쉬지 않고 돌아가는 운영 환경에서는 **"바뀐 것만 정확히 고치는 것"** 못지않게 **"커밋마다 쌓이는 작은 파일과 과거 스냅샷·메타데이터를 안전하게 치우는 것"**이 중요합니다. 이번 회차는 전반부(약 57분)에서 스터디 공통 환경의 실제 디스크 포화 사고와 `lake.lesson07` 유지보수 실험을 다루고, 후반부(약 48분)에서 각자의 개인 프로젝트 설계를 9개 질문으로 점검합니다(설계: [`labs/README.md`](../README.md)).

수업 중에는 미리 준비된 데모 테이블을 Lakehouse Explorer 화면으로 열어 보며 결과를 예측·확인하고 개인 프로젝트 리뷰를 진행합니다. 집에서는 [8절](#8-집에서-따라-하기) 안내에 따라 개인 Docker 환경에서 같은 실험을 재현할 수 있습니다. 수업 중 제출할 파일은 없으며 과제 형식과 기한은 별도 공지를 따릅니다.

이 가이드에서 쓰는 표시는 네 가지입니다.

- **실측**: 스터디 공통 환경(Spark 3.5.6, Iceberg 1.10.0, Flink 1.20)에서 실행해 확인한 값이며 날짜를 함께 적습니다. 개인 환경에서는 스냅샷 ID, 시각, 세부 파일 크기가 다를 수 있습니다.
- **코드**: 저장소 파일이나 오픈소스 엔진 소스에 적힌 구현입니다.
- **문서**: 해당 버전 공식 문서에 적힌 동작과 권고 사항입니다.
- **설계**: 수업·코드에서 직접 실행하지 않은 운영 기준, 처방안, 개인 프로젝트 적용 기준입니다.

---

## 1. 이 회차에서 답할 실무 질문 세 가지

이번 회차의 실험과 프로젝트 리뷰는 아래 세 가지 질문에 답하는 과정입니다(설계).

1. **데이터는 몇십 행뿐인데 왜 저장소 용량이 수 GB씩 차오르고, 어디서부터 줄여야 하나요?**
   - 스트리밍 체크포인트 주기, 빈 커밋(empty commit), Iceberg 메타데이터 파일(`metadata.json`) 보존 속성이 맞물리는 메커니즘을 확인합니다(실측·코드·문서).
2. **`rewrite_data_files`(압축)를 돌렸는데 왜 저장소 용량이 바로 줄지 않고, 어떤 순서로 정리해야 하나요?**
   - Iceberg의 파일 계층(`metadata.json` → `manifest list` → `manifest` → `data file`)에서 각 유지보수 프로시저가 지우는 대상과 시점을 구분합니다(실측·문서).
3. **스냅샷을 지울 때 6회차 증분 파이프라인(`progress`)이나 실행 중인 Flink 잡이 깨지지 않게 하려면 어떤 기준선을 지켜야 하나요?**
   - 하류 소비자의 마지막 성공 스냅샷(`progress`)과 Flink 최신 스냅샷을 보호하는 보존 기준을 세웁니다(실측·코드·문서·설계).

---

## 2. 사고 보고: 데이터 27행, 메타데이터 3.6GB

### 스터디 공통 환경에서 일어난 일 (스터디 공통 환경 실측 2026-11-05)

2026년 11월 5일 저녁, 스터디 공통 환경의 디스크 사용률이 가득 차면서 Spark Thrift Server의 새 세션이 열리지 않고 OpenMetadata 검색 인덱스(Elasticsearch)가 읽기 전용 모드로 잠기는 장애가 발생했습니다(스터디 공통 환경 실측 2026-11-05).

원인을 찾기 위해 레이크하우스 저장소(`warehouse`)의 테이블별 실제 데이터와 메타데이터 크기를 비교한 결과는 다음과 같았습니다(스터디 공통 환경 실측 2026-11-05).

| 비교 항목 | `bronze.orders` (Flink 적재) | `bronze.payments` / `refunds` | `silver.orders` (배치 MERGE) |
|---|---|---|---|
| 실제 데이터 행 수 · 파일 수 | **27행** · 데이터 파일 1개 | 63행 이하 · 데이터 파일 각 1개 | 27행 · 데이터 파일 1개 |
| 데이터 파일 총크기 | **약 5~8 KB** | 약 5~8 KB | 수 KB |
| 스냅샷(`snapshots`) 개수 | **2,952개** (11/1 07:51 ~ 11/4 16:39 UTC) | 약 2,950개 | **907개** |
| 저장소 `metadata.json` 객체 수 | **2,953개** (8 KB → 최근 **2.4 MB**) | 각 약 2,950개 | **101개** |
| 메타데이터 총크기 | **약 3.6 GB** | 두 테이블 합 약 7.2 GB | **약 110 MB** |

세 Bronze 테이블(`orders`, `payments`, `refunds`)의 메타데이터만 약 **10.8 GB**(이전 `cdc_events` 테이블 포함 Bronze 전체 약 **12.7 GB**)를 차지하고 있었습니다(스터디 공통 환경 실측 2026-11-05).

### 원인 ① 데이터가 없어도 약 100초마다 커밋한다 (코드·실측)

`bronze.orders`의 스냅샷 2,952개를 집계해 보니 `operation`은 모두 `append`였고, 전체 스냅샷의 `added-records` 합계는 **27건**뿐이었습니다. 즉 **2,900개가 넘는 스냅샷이 새로 들어온 행이 0건인 빈 커밋(empty commit)**이었습니다(스터디 공통 환경 실측 2026-11-05).

- **체크포인트 주기**: 저장소 [`compose.yaml`](../../compose.yaml)의 `CHECKPOINT_INTERVAL` 기본값은 **`10s`(10초)**입니다(코드: [`compose.yaml`](../../compose.yaml), [`flink/ingest.sql.template`](../../flink/ingest.sql.template)).
- **Flink Iceberg Sink의 빈 커밋 동작**: Iceberg 1.10.0의 Flink 1.20 Sink(`IcebergFilesCommitter`)는 테이블 속성 `flink.max-continuous-empty-commits` 기본값을 **`10`**으로 둡니다. 새로 쓸 데이터 파일이 없더라도 체크포인트가 10번 연속 비면 10번째마다 빈 스냅샷을 커밋합니다(코드: [IcebergFilesCommitter.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java)).
- **실측 간격과의 일치**: 체크포인트 10초 × 10회 = 약 100초마다 스냅샷이 하나씩 생겼고, 실측된 스냅샷 평균 간격(약 **99초**)과 일치합니다(스터디 공통 환경 실측 2026-11-05).

### 원인 ② `metadata.json`이 지워지지 않고 파일마다 커진다 (문서·실측)

스냅샷이 많아도 왜 `silver.orders`(스냅샷 907개, 110MB)보다 `bronze.orders`(스냅샷 2,952개, 3.6GB)가 훨씬 크게 불어났을까요?

- **`metadata.json`의 구조**: 커밋할 때마다 새 `metadata.json` 파일이 하나씩 만들어집니다. 만료(`expire_snapshots`)를 하지 않으면 각 `metadata.json` 안에 **지금까지의 전체 스냅샷 목록**이 그대로 담기므로, 처음에 **8 KB**였던 파일 크기가 2,953번째에는 파일 하나당 **2.4 MB**까지 커졌습니다(스터디 공통 환경 실측 2026-11-05, 문서: [Iceberg Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/)).
- **기본 메타데이터 보존 속성**: Iceberg 1.10.0의 기본 설정은 `write.metadata.delete-after-commit.enabled = false`, `write.metadata.previous-versions-max = 100`입니다(문서: [Iceberg Configuration](https://iceberg.apache.org/docs/1.10.0/configuration/)).
- **추적 목록 밖으로 밀려난 고아 파일**: 기본값에서는 메타데이터 이력 테이블(`metadata_log_entries`)만 최근 100개 + 현재 1개 = **101개**를 기억하고, 목록에서 밀려난 이전 `metadata.json` 파일 **2,852개**는 객체 저장소(MinIO)에서 지우지 않은 채 그대로 남깁니다(스터디 공통 환경 실측 2026-11-05·2026-11-07).

### 연쇄 피해: 눈에 띄는 증상이 진짜 원인은 아니다 (실측)

디스크가 가득 찬 직후 가장 시끄럽게 에러를 뿜은 곳은 Iceberg가 아니라 **Debezium Kafka Connect**였습니다(스터디 공통 환경 실측 2026-11-05).

1. Bronze 메타데이터 누적으로 디스크 여유 공간이 고갈됨
2. Kafka 브로커가 로그 디렉터리에 쓰지 못해 `No space left on device` 발생
3. Debezium Connect가 Kafka 클러스터 메타데이터를 받지 못해 `"Rebootstrapping"` 재연결 로그를 **분당 약 77 MB씩 총 2.1 GB** 기록함

겉으로 드러난 Connect 로그 폭증만 보고 Connect 로그만 지웠다면 몇 시간 뒤 다시 디스크가 찼을 것입니다. 장애를 볼 때는 **연쇄 피해(증상)와 저장소 바닥의 근본 원인(Bronze 메타데이터 누적)을 구분**해야 합니다(실측·설계).

---

## 3. Iceberg 파일 계층과 유지보수 도구 네 가지

Iceberg 테이블은 네 층의 파일로 이루어져 있으며, 유지보수 도구마다 정리하는 층과 실제로 저장소 공간이 줄어드는 시점이 다릅니다(문서: [Iceberg Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/), [Spark Procedures](https://iceberg.apache.org/docs/1.10.0/spark-procedures/)).

```text
[카탈로그 포인터]
   └── metadata.json (테이블 스키마, 파티션, 전체 snapshot 목록, metadata_log_entries)
          └── snap-*.avro (manifest list: 특정 snapshot을 이루는 manifest 목록)
                 └── *-m*.avro (manifest file: 데이터 파일 경로와 열별 min/max 통계)
                        └── *.parquet (data file: 실제 행 데이터)
```

| 도구 (Spark 프로시저 / 속성) | 무엇을 바꾸거나 지우나요? | 언제 저장소 공간이 줄어드나요? | 운영 주의점 (코드·문서·실측) |
|---|---|---|---|
| `rewrite_data_files` (compact) | 작은 데이터 파일(`.parquet`) 여러 개를 읽어 큰 파일로 다시 씀 (새 스냅샷 생성) | **바로 줄지 않음** (이전 스냅샷이 옛 파일을 계속 참조해 오히려 새 파일만큼 일시 증가) | 이후 `expire_snapshots`가 돌아야 옛 데이터 파일이 지워짐 (실측·문서) |
| `expire_snapshots` (expire) | 기준 시각(`older_than`) 이전이면서 보존 개수(`retain_last`) 밖인 **스냅샷**을 만료하고, 더 이상 어떤 스냅샷도 참조하지 않는 옛 데이터 파일·매니페스트·매니페스트 리스트를 삭제 | **실행 즉시** 옛 `.parquet`과 만료된 스냅샷의 `.avro`가 지워짐 (`metadata.json`은 지우지 않음) | 6회차 증분 `progress` 스냅샷이나 Flink의 최신 스냅샷을 지우면 하류 작업이 깨짐 (실측·문서) |
| `remove_orphan_files` (orphans) | 현재 어떤 스냅샷·메타데이터 이력도 참조하지 않는 저장소의 **고아 파일**(추적 밖으로 밀려난 옛 `metadata.json`, 실패한 작업의 찌꺼기 파일 등)을 삭제 | `older_than` 기준보다 오래된 고아 파일이 있을 때 삭제 | Iceberg 1.10은 동시 쓰기 보호를 위해 **`older_than`이 24시간보다 최근이면 거부**함(기본 3일). S3FileIO 환경에서는 `prefix_listing => true` 필요 (실측·코드·문서) |
| `rewrite_manifests` (manifests) | 현재 스냅샷의 작은 매니페스트(`*-m*.avro`)들을 합쳐 쿼리 계획 수립 부담을 줄임 | 새 매니페스트가 추가되고, 옛 매니페스트는 다음 `expire_snapshots` 뒤에 정리됨 | 대량 커밋·압축·만료 뒤에 실행해 현재 매니페스트 개수를 줄임 (실측·문서) |
| 테이블 속성 `write.metadata.delete-after-commit.enabled` + `previous-versions-max` | 커밋할 때마다 보존 한도(`previous-versions-max`, 기본 100) 밖으로 밀려난 가장 오래된 `metadata.json`을 자동 삭제 | 커밋 시점에 한도를 넘긴 `metadata.json`이 바로 지워짐 | 기본값이 `false`이므로 테이블 생성·운영 시 명시적으로 켜야 `metadata.json` 고아 파일 누적을 예방함 (실측·문서) |

---

## 4. `lesson07` 실험판 등장인물과 데모 범례

공통 파이프라인 테이블(`lake.bronze.*`, `lake.silver.*`, `lake.gold.*`)을 건드리지 않기 위해 7회차 실습은 `lake.lesson07` 네임스페이스 아래에 실행 ID(`--run-id`)마다 세 개의 테이블을 만듭니다(코드: [`lab.py`](lab.py)).

### 테이블 세 개 (`plain` / `tuned` / `inc`)

| 테이블 구분 | 실제 이름 (`--run-id s07-demo-grow` 예시) | 적용된 테이블 속성 (`TBLPROPERTIES`) | 실험에서의 역할 |
|---|---|---|---|
| `plain` | `lake.lesson07.s07_demo_grow_plain` | 기본값 (`delete-after-commit.enabled = false`, `previous-versions-max = 100`) | 공통 Bronze 테이블과 같은 기본 상태 재현 (추적 밖 `metadata.json`이 저장소에 남음) |
| `tuned` | `lake.lesson07.s07_demo_grow_tuned` | `'write.metadata.delete-after-commit.enabled'='true'`, `'write.metadata.previous-versions-max'='10'` | 커밋 시 오래된 `metadata.json`을 자동 삭제해 최대 11개(이전 10 + 현재 1)만 유지 |
| `inc` | `lake.lesson07.s07_demo_trap_inc` | 기본값 | 6회차 증분 파이프라인의 마지막 성공 위치(`progress`) 만료 함정 실험 (`progress-trap`) |

- `plain`과 `tuned`의 스키마는 `event_id STRING, order_id STRING, amount BIGINT, event_ts TIMESTAMP`로 같습니다(코드: [`lab.py`](lab.py) 22행).
- `grow` 단계에서는 스트리밍 체크포인트마다 작은 커밋이 쌓이는 상황을 재현하기 위해 1행씩 **120번** `INSERT`합니다(100번을 넘겨야 `plain`의 기본 추적 한도 101개 밖으로 밀려나는 `metadata.json`을 관찰할 수 있습니다). 이어서 유지보수 전후 데이터 불변성을 검증하기 위해 기준 행 수(`lesson07.grow.rows = '120'`)와 금액 합계(`lesson07.grow.checksum = '726000'`)를 `ALTER TABLE ... SET TBLPROPERTIES`로 기록하며, 이때 스냅샷 추가 없이 `metadata.json` 버전만 1회 더 올라갑니다(코드: [`lab.py`](lab.py) 124~145행).

### `demo.sh` 고정 `run-id` 5세트 범례

수업 시연과 Lakehouse Explorer 예제는 [`demo.sh`](demo.sh)가 미리 만들어 둔 다섯 세트의 데모 테이블을 단계별 타임캡슐처럼 열어 봅니다(코드: [`demo.sh`](demo.sh) 32~36행).

| 데모 `run-id` | 멈추는 단계 (`--stop-after`) | 커밋 수 | 무엇을 비교하나요? |
|---|---|---|---|
| `s07-demo-grow` | `grow` | 120 | 120번 커밋 직후 `plain` vs `tuned`의 스냅샷·메타데이터 이력·작은 파일 120개 |
| `s07-demo-compact` | `compact` | 120 | `rewrite_data_files` 직후 현재 파일(1개)과 과거 스냅샷이 붙잡고 있는 전체 파일(121개) |
| `s07-demo-expire` | `expire` | 120 | `expire_snapshots` 직후 남은 스냅샷(1개)과 실제 데이터 파일 삭제(121 → 1개) |
| `s07-demo-clean` | `manifests` | 120 | `orphans` → `rewrite_manifests`까지 마친 뒤 현재 매니페스트(1개)와 메타데이터 크기 |
| `s07-demo-trap` | `progress-trap` | 5 (`inc`는 6) | 6회차 증분 시작점(`progress`)을 지켰을 때와 만료시켰을 때의 `create_changelog_view` 결과 |

---

## 5. 단계별 실험 결과와 Lakehouse Explorer 예제

스터디 공통 환경에서 `demo.sh`를 실행해 측정한 메타데이터 테이블 집계와 MinIO 객체 저장소 실물 수치입니다(스터디 공통 환경 실측 2026-11-07). 모든 단계에서 `plain`과 `tuned`의 데이터 건수(`rows = 120`)와 금액 합계(`checksum = 726,000`)는 변하지 않고 `match: true`를 유지했습니다(실측·코드).

> **메타데이터 테이블 조회 시 주의점 (`DISTINCT`)**: Iceberg의 `.all_data_files`와 `.all_manifests` 테이블은 남아 있는 여러 스냅샷이 같은 파일을 참조할 때 스냅샷마다 중복해서 행을 보여 줍니다. 실제로 `grow` 직후 `.all_data_files`를 단순 `count(*)`로 세면 219행이 나오지만 고유 파일 경로(`count(DISTINCT file_path)`)는 **120개**입니다(스터디 공통 환경 실측 2026-11-07, 코드: [`lab.py`](lab.py) 238~245행, 문서: [Inspecting tables](https://iceberg.apache.org/docs/1.10.0/spark-queries/#inspecting-tables)).

### 단계별 핵심 지표 비교 (`plain` vs `tuned`, 스터디 공통 환경 실측 2026-11-07)

| 단계 (`run-id`) | 구분 | `snapshots` | 현재 `files` (`.parquet`) | `all_data_files` (`DISTINCT`) | 현재 `.manifests` | `metadata_log` (이력 테이블) | 저장소 `metadata.json` 실물 개수 (크기) | 저장소 데이터 객체 수 (크기) |
|---|---|---|---|---|---|---|---|---|
| **① `grow`** (`s07-demo-grow`) | `plain` | 120 | 120 (평균 1,228 B) | 120 | 21 | **101** | **122개** (약 11.0 MB) | 120개 (976 KB) |
| | `tuned` | 120 | 120 (평균 1,228 B) | 120 | 21 | **11** | **11개** (약 4.1 MB) | 120개 (984 KB) |
| **② `compact`** (`s07-demo-compact`) | `plain` | 121 | **1** | **121** | 22 | 101 | 123개 (약 11.5 MB) | **121개** (988 KB) |
| | `tuned` | 121 | **1** | **121** | 22 | 11 | 11개 (약 4.4 MB) | **121개** (988 KB) |
| **③ `expire`** (`s07-demo-expire`) | `plain` | **1** | 1 | **1** | 22 | 101 | 124개 (약 8.6 MB) | **1개** (28 KB) |
| | `tuned` | **1** | 1 | **1** | 22 | 11 | 11개 (약 1.4 MB) | **1개** (24 KB) |
| **④ `orphans` + `manifests`** (`s07-demo-clean`) | `plain` | 2 | 1 | 1 | **1** (`all` 23) | 101 | **125개** (약 8.6 MB) | 1개 (24 KB) |
| | `tuned` | 2 | 1 | 1 | **1** (`all` 23) | 11 | **11개** (약 1.3 MB) | 1개 (24 KB) |

### 프로시저별 반환 결과와 핵심 해석 (실측·코드·문서)

1. **`grow` 직후 (`plain` vs `tuned`)**:
   - 120번 `INSERT`와 기준값 속성 저장(`ALTER TABLE`) 1번으로 총 122개의 `metadata.json`(버전 `00000`~`00121`)이 작성되었습니다(실측·코드).
   - `plain`은 `metadata_log_entries` 테이블에 101개만 남기고 앞선 21개를 추적 목록에서 밀어냈지만, 저장소에는 **122개(약 11.0 MB)**가 모두 남아 있습니다. 반면 `tuned`는 커밋할 때마다 오래된 파일을 지워 **11개(`00111`~`00121`, 약 4.1 MB)**만 남았습니다(스터디 공통 환경 실측 2026-11-07).
2. **`compact` (`rewrite_data_files`)**:
   - 프로시저 반환값: `rewritten_data_files_count = 120`, `added_data_files_count = 1`, `rewritten_bytes_count = 147,337`(스터디 공통 환경 실측 2026-11-07).
   - 현재 스냅샷이 읽는 데이터 파일(`.files`)은 120개에서 **1개**로 줄었지만, 이전 120개 스냅샷이 타임 트래블과 복구를 위해 옛 파일 120개를 그대로 참조하므로 `.all_data_files`(`DISTINCT`)와 저장소 데이터 객체 수는 **121개**가 됩니다(실측·문서). **압축만 하고 스냅샷 만료를 안 하면 디스크 공간은 줄지 않습니다.**
3. **`expire` (`expire_snapshots`)**:
   - 프로시저 반환값: `deleted_data_files_count = 120`, `deleted_manifest_files_count = 120`, `deleted_manifest_lists_count = 120`(스터디 공통 환경 실측 2026-11-07).
   - 이전 스냅샷 120개가 만료되면서 비로소 옛 데이터 파일 120개와 매니페스트 리스트(`snap-*.avro`) 120개가 지워지고, 저장소 데이터 객체가 **1개(24~28 KB)**로 줄어듭니다(실측).
   - 단, `expire_snapshots`는 `metadata.json` 파일을 지우지 않으므로 `plain`의 `metadata.json`은 오히려 124개로 늘어납니다(실측·문서).
4. **`orphans` (`remove_orphan_files`)와 24시간 안전장치**:
   - 방금 만든 실험 테이블에서는 `older_than`(현재 시각 - 24시간 5분)보다 오래된 파일이 없어 조회·삭제 모두 **0개**가 나옵니다(스터디 공통 환경 실측 2026-11-07, 코드: [`lab.py`](lab.py) 163~183행).
   - Iceberg 1.10.0의 `RemoveOrphanFilesProcedure`는 동시에 진행 중인 쓰기 작업이 아직 커밋하지 않은 새 데이터·메타데이터 파일을 고아로 오인해 지우지 않도록, **`older_than`이 현재 시각으로부터 24시간 이내이면 `dry_run`이라도 실행을 거부**합니다(코드: [RemoveOrphanFilesProcedure.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RemoveOrphanFilesProcedure.java) `validateInterval`).
   - 반면 며칠 동안 쌓인 공통 환경의 실제 Bronze 테이블 3개에 `older_than => TIMESTAMP '2026-11-06 04:00:00'`, `dry_run => true`, `prefix_listing => true`로 고아 파일 목록만 조회해 본 결과, 테이블마다 **2,863~2,865개**(`metadata.json` **2,852~2,854개** + `avro` **11개**)가 삭제 후보로 확인되었습니다(스터디 공통 환경 실측 2026-11-07, 조회만 수행하고 삭제하지 않음).
5. **`manifests` (`rewrite_manifests`)**:
   - 프로시저 반환값: `rewritten_manifests_count = 22`, `added_manifests_count = 1`(스터디 공통 환경 실측 2026-11-07).
   - 현재 스냅샷이 참조하는 매니페스트(`.manifests`)가 22개에서 **1개**로 합쳐집니다. 방금 합치기 전의 매니페스트 22개는 직전 스냅샷이 참조하고 있어 `.all_manifests`(`DISTINCT`)는 23개이며, 다음 `expire_snapshots` 주기에 함께 정리됩니다(실측·문서).

### Lakehouse Explorer 7회차 예제 8개와 확인할 값

Lakehouse Explorer(`http://localhost:28090`)의 **SQL** 탭에서 바로 실행할 수 있는 7회차 예제 8개입니다(코드: [`tools/explorer/README.md`](../../tools/explorer/README.md), 스터디 공통 환경 실측 2026-11-07).

| 예제 이름 | 조회 대상 데모 테이블 | 실행 결과에서 확인할 핵심 수치 (실측 2026-11-07) |
|---|---|---|
| `7회차 · grow snapshot·메타데이터 비교` | `s07_demo_grow_{plain,tuned}` | `snapshots` 둘 다 `120` / `metadata_log`는 `plain` `101`, `tuned` `11` |
| `7회차 · grow plain 최근 snapshot 5개` | `s07_demo_grow_plain.snapshots` | 최근 5개 모두 `operation = 'append'`, `added_records = '1'` |
| `7회차 · grow plain 데이터 파일 크기` | `s07_demo_grow_plain.files` | `data_files = 120`, `avg_bytes ≈ 1227.8` (약 1.2 KB짜리 작은 파일 120개) |
| `7회차 · compact 현재 파일과 과거 파일` | `s07_demo_compact_{plain,tuned}` | 둘 다 현재 `files = 1`, `all_data_files_distinct = 121` |
| `7회차 · expire 남은 snapshot과 파일` | `s07_demo_expire_{plain,tuned}` | 둘 다 `snapshots = 1`, `all_data_files_distinct = 1` |
| `7회차 · clean 현재·전체 매니페스트` | `s07_demo_clean_{plain,tuned}` | 둘 다 현재 `manifests = 1`, `all_manifests_distinct = 23` |
| `7회차 · trap inc 남은 snapshot` | `s07_demo_trap_inc.snapshots` | 위험한 만료 뒤 마지막 스냅샷 1개만 남고 `parent_id`가 끊긴 상태 |
| `7회차 · grow tuned 메타데이터 이력 · 최대 11개` | `s07_demo_grow_tuned.metadata_log_entries` | `00111-...metadata.json`부터 `00121-...metadata.json`까지 11개만 유지 |

---

## 6. 6회차 연결: 증분 시작점(`progress`)을 지우면 생기는 일 (`progress-trap`)

6회차에서 Gold 증분 파이프라인은 Silver 현재 상태 테이블의 **마지막 성공 스냅샷 ID(`progress`)**를 기억해 두었다가, 다음 실행 때 `create_changelog_view(start-snapshot-id => progress, end-snapshot-id => main)`로 그사이 변경만 읽었습니다(코드: [`labs/session06/lab.py`](../session06/lab.py)). 만약 7회차의 `expire_snapshots`가 이 `progress` 스냅샷까지 지워 버리면 어떻게 될까요?

[`lab.py`](lab.py)의 `progress_trap` 단계는 `inc` 테이블에 1~3번 행을 커밋한 뒤 **두 번째 스냅샷(`progress`, 커밋 시각 `committed_at`)**을 마지막 성공 위치로 기억하고, 이어서 4~6번 행을 더 커밋한 뒤(최신 스냅샷 `end`) 두 가지 만료 방식을 차례로 시험합니다(코드: [`lab.py`](lab.py) 214~234행, 스터디 공통 환경 실측 2026-11-07).

| 구분 | 만료 호출 기준 (`expire_snapshots`) | 남은 스냅샷 | `create_changelog_view(start=progress, end=end)` 결과 (실측 2026-11-07) |
|---|---|---|---|
| **① 안전한 정리 (`safe`)** | `older_than => TIMESTAMP '<progress의 committed_at>'`, `retain_last => 1` | `progress`(2번째)부터 6번째까지 보존 (1번째만 만료) | **성공 (`succeeded: true`)** — 3~6번째 커밋의 변경 **4행(`rows = 4`)** 정상 조회 |
| **② 위험한 정리 (`unsafe`)** | `older_than => current_timestamp()`, `retain_last => 1` | 최신(6번째) 스냅샷 **1개만** 남고 `progress` 스냅샷 만료됨 | **실패 (`succeeded: false`)** — `IllegalArgumentException: Starting snapshot (exclusive) ... is not a parent ancestor of end snapshot ...` |

### 안전한 스냅샷 보존 기준 (문서·설계)

- **`older_than` 상한선**: 스냅샷 만료 기준 시각(`older_than`)은 반드시 **모든 하류 증분 소비자의 `progress` 스냅샷 커밋 시각보다 이전**이어야 하며, 야간·주말 장애 복구 시간을 고려해 충분한 여유(예: 3~7일)를 둡니다(문서: [Iceberg Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/), 설계).
- **만료 사고 발생 시 비상 복구**: 만약 장기 장애로 `progress` 스냅샷이 이미 만료되었다면 증분 changelog 읽기는 불가능하므로, 5회차처럼 **현재 최신 스냅샷 기준으로 전체 재계산(full refresh)을 1회 실행하고 그 최신 스냅샷 ID로 `progress`를 다시 세팅**하는 비상 경로를 둡니다(설계).

---

## 7. 운영 기준 체크리스트와 Flink 테이블 주의점

### 유지보수 순서·주기·보존·속성·지표 체크리스트 (실측·코드·문서·설계)

| 점검 영역 | 운영 기준 | 근거 및 이유 |
|---|---|---|
| **① 실행 순서** | `rewrite_data_files` → `expire_snapshots` → `remove_orphan_files` → `rewrite_manifests` | 압축(`compact`)을 먼저 해야 만료(`expire`) 때 옛 작은 데이터 파일들이 한꺼번에 삭제됨 (실측·설계, 각 프로시저 설명은 문서: [Iceberg Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/)) |
| **② 테이블 속성** | 테이블 생성·운영 시 `write.metadata.delete-after-commit.enabled = 'true'`, `write.metadata.previous-versions-max = '10'~'100'` 설정 | 기본값(`false`)으로 두면 추적 목록 밖으로 밀려난 `metadata.json`이 저장소에 계속 쌓임 (실측·문서: [Iceberg Configuration](https://iceberg.apache.org/docs/1.10.0/configuration/)) |
| **③ 스냅샷 보존 기준** | `older_than` < `min(하류 progress 스냅샷 시각, 안전 보존 기간)` + Flink 최신 스냅샷 보존 | 증분 파이프라인의 시작 스냅샷(`progress`)과 Flink 체크포인트 복구용 스냅샷 보호 (실측·문서·설계) |
| **④ 빈 커밋 억제** | Flink 체크포인트 간격과 `flink.max-continuous-empty-commits` 조정 (유휴 시간대 빈 커밋 폭증 방지) | 10초 체크포인트 × 기본 10회 = 약 100초마다 빈 스냅샷과 새 `metadata.json`이 생성됨 (실측·코드: [IcebergFilesCommitter.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/flink/v1.20/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergFilesCommitter.java), 설계) |
| **⑤ 고아 파일 정리** | `older_than`을 최소 24시간 이전(권장 3일 이전)으로 두고 `dry_run => true` 확인 후 실행 (`prefix_listing => true`) | 동시 쓰기 중인 미커밋 파일 보호 및 S3FileIO 목록 조회 호환 (실측·코드·문서: [Spark Procedures](https://iceberg.apache.org/docs/1.10.0/spark-procedures/#remove_orphan_files)) |
| **⑥ 감시 지표와 검증** | 메타데이터 테이블(`.snapshots`, `.files`, `.metadata_log_entries`), 저장소 용량, 유지보수 전후 행 수·합계 대조 | 유지보수는 물리 파일만 정리할 뿐 쿼리 결과를 바꾸면 안 됨 (실측·코드·설계) |

### Flink가 쓰는 테이블을 정리할 때 주의할 점 (문서)

Apache Iceberg 1.10.0 공식 문서([Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/))는 Flink 스트리밍 잡이 적재하는 테이블에서 유지보수를 돌릴 때 다음 사항을 경고합니다(문서).

- **스냅샷 만료 주의**: Flink Iceberg Sink는 체크포인트가 완료될 때 스냅샷 summary에 `flink.job-id`와 체크포인트 ID를 기록해 정확히 한 번(exactly-once) 커밋 여부를 확인합니다. `expire_snapshots`로 **실행 중인 Flink 잡이 만든 가장 최근 스냅샷까지 지워 버리면 Flink 작업 상태 복구가 깨질 수 있으므로**, 최신 스냅샷은 반드시 남겨 두어야 합니다(문서: [Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/)).
- **고아 파일 삭제 주의**: Flink가 데이터 파일을 먼저 저장소에 쓰고 아직 체크포인트 커밋을 마치지 않은 상태에서 `remove_orphan_files`가 너무 최근 파일까지 지우면 Flink 작업 상태가 깨질 수 있습니다. 따라서 충분히 오래된 기준 시각(`older_than`)만 사용합니다(문서).
- **커밋 지연 감시 지표**: Flink Sink 메트릭 중 `elapsedSecondsSinceLastSuccessfulCommit`을 감시하면 체크포인트나 카탈로그 문제로 커밋이 멈춘 이상 상태를 빨리 감지할 수 있습니다(문서: [Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/)).

---

## 8. 집에서 따라 하기

### 먼저 확인할 것

모든 명령은 **개인 컴퓨터의 저장소 루트**에서 실행합니다. 최신 코드를 받은 뒤 기본 컨테이너 스택(Spark Thrift Server, Iceberg 카탈로그 DB, MinIO, `runner`)이 떠 있는지 확인합니다(코드: [`lab.sh`](lab.sh), [`demo.sh`](demo.sh)).

```bash
git pull --ff-only
./scripts/lab.sh ps
```

- `--run-id`는 반드시 `s07-`로 시작하고 영문 소문자·숫자·밑줄(`_`)·하이픈(`-`)만 써서 최대 40자로 지정합니다(코드: [`lab.py`](lab.py) 54~55행).
- 한 `run-id`는 한 사람이 순서대로 실행하며, 실험 중에는 그 `run-id`의 테이블에 동시에 쓰는 다른 작업이 없어야 합니다(코드: [`lab.py`](lab.py) 4~5행).

### ① `demo.sh`로 데모 5세트 준비하고 Explorer로 확인하기

[`demo.sh`](demo.sh)는 고정 `run-id` 5개(`s07-demo-grow`, `s07-demo-compact`, `s07-demo-expire`, `s07-demo-clean`, `s07-demo-trap`)를 한 번에 만듭니다(스터디 공통 환경 실측 2026-11-07: 약 90초 소요). 이 이름들로 테이블이 준비되어야 **Lakehouse Explorer의 `7회차 · ...` 예제 8개가 수정 없이 바로 실행됩니다**(코드: [`demo.sh`](demo.sh), [`tools/explorer/README.md`](../../tools/explorer/README.md)).

```bash
# 처음 실행할 때 (기존 데모 테이블이 있으면 멈추고 안내함)
./labs/session07/demo.sh

# 기존 데모 5세트만 지우고 다시 만들며 JSONL 로그까지 남길 때
./labs/session07/demo.sh --fresh --log /tmp/s07-demo.jsonl
```

이어서 Lakehouse Explorer를 띄우고 브라우저에서 `http://localhost:28090`을 엽니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --build --no-deps explorer
```

- **SQL** 탭 왼쪽 예제 목록에서 [5절 표](#lakehouse-explorer-7회차-예제-8개와-확인할-값)에 정리된 `7회차 · ...` 예제 8개를 차례로 눌러 실행합니다.
- 실습 데이터를 남긴 채 Explorer 컨테이너만 내리려면 아래 명령을 실행합니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml rm -sf explorer
```

### ② `lab.sh`로 내 `run-id`를 단계별로 실행하고 비교하기

[`lab.sh`](lab.sh)로 자신만의 `RUN_ID`를 지정해 한 번에 실행(`all`)하거나 단계별로 끊어 실행할 수 있습니다(스터디 공통 환경 실측 2026-11-07: `all` 120커밋 기준 약 27초 소요, 코드: [`lab.py`](lab.py) 306~330행).

```bash
# 1) 전 단계(prepare→grow→compact→expire→orphans→manifests→progress-trap→verify) 한 번에 실행
RUN_ID="s07-all-$(date +%Y%m%d-%H%M%S)"
./labs/session07/lab.sh all --run-id "$RUN_ID"

# 2) 단계별로 끊어 실행하며 JSON 출력 확인하기
RUN_STEP="s07-step-$(date +%Y%m%d-%H%M%S)"
./labs/session07/lab.sh prepare --run-id "$RUN_STEP"
./labs/session07/lab.sh grow --commits 120 --run-id "$RUN_STEP"
./labs/session07/lab.sh compact --run-id "$RUN_STEP"
./labs/session07/lab.sh expire --run-id "$RUN_STEP"
./labs/session07/lab.sh orphans --run-id "$RUN_STEP"
./labs/session07/lab.sh manifests --run-id "$RUN_STEP"
./labs/session07/lab.sh progress-trap --run-id "$RUN_STEP"
./labs/session07/lab.sh verify --run-id "$RUN_STEP"

# 3) 현재 상태만 다시 측정할 때
./labs/session07/lab.sh measure --run-id "$RUN_STEP"

# 4) 실습이 끝난 개별 run-id의 지정 테이블 3개(plain, tuned, inc)만 삭제할 때
./labs/session07/lab.sh clean --run-id "$RUN_STEP"
```

### 액션별 목적과 JSON 출력에서 확인할 곳 (코드: [`lab.py`](lab.py))

| `action` (옵션) | 하는 일 | 출력 JSON에서 확인할 핵심 필드 |
|---|---|---|
| `prepare` | `lake.lesson07` 아래 `plain`, `tuned`(자동 삭제 속성), `inc` 테이블 3개 생성 | `tables`, 직후 `measure`(`after: "prepare"`) |
| `grow [--commits 120]` | `plain`·`tuned`에 1행씩 N번 커밋 후 기준 행 수·합계 속성 저장 | `commits`, `expected`(`rows: 120, checksum: 726000`), `measure`의 `metadata_log`(`101` vs `11`) |
| `compact` | `rewrite_data_files`(`min-input-files=2`) 실행 | `rewritten_data_files_count: 120`, `added_data_files_count: 1`, `measure`의 `data_files: 1`, `all_data_files_distinct: 121` |
| `expire` | 현재 시각 기준 `expire_snapshots`(`retain_last=1`) 실행 | `deleted_data_files_count: 120`, `measure`의 `snapshots: 1`, `all_data_files_distinct: 1` |
| `orphans` | 서버 시각 - 24시간 5분 기준 `remove_orphan_files`(`prefix_listing => true`) `dry_run` 후 실행 | `dry_run: true/false`, `counts`(방금 만든 테이블은 모두 `0`) |
| `manifests` | `rewrite_manifests` 실행 | `rewritten_manifests_count: 22`, `added_manifests_count: 1`, `measure`의 `manifests: 1` |
| `progress-trap` | `inc` 테이블에서 안전한 만료(`safe`)와 위험한 만료(`unsafe`) 뒤 changelog 조회 비교 | `safe`(`succeeded: true, rows: 4`), `unsafe`(`succeeded: false, error: "... is not a parent ancestor ..."`) |
| `verify` / `measure` | 현재 메타데이터 지표 측정 및 `grow` 기준값(`rows`, `checksum`) 일치 확인 | `match: true` |
| `all [--commits N] [--stop-after 단계]` | `prepare`부터 지정 단계(기본 `verify`)까지 연속 실행 | 전 단계 JSON 출력 및 마지막 `verify` `match: true` |
| `clean` | 해당 `run-id`의 `plain`, `tuned`, `inc` 3개만 `DROP TABLE IF EXISTS ... PURGE` | `tables` |

### 멈췄을 때 오류 확인하기 (실측·코드·문서)

| 증상·오류 메시지 | 원인과 해결 방법 |
|---|---|
| `No FileSystem for scheme "s3"` (`remove_orphan_files` 직접 호출 시) | S3FileIO를 쓰는 이 환경에서 `prefix_listing => true` 없이 `remove_orphan_files`를 부르면 Hadoop `s3` FileSystem으로 디렉터리 목록을 읽으려다 실패합니다. 반드시 **`prefix_listing => true`** 인자를 함께 넘깁니다(스터디 공통 환경 실측 2026-11-07, 코드: [`lab.py`](lab.py) 89~94행, 문서: [remove_orphan_files](https://iceberg.apache.org/docs/1.10.0/spark-procedures/#remove_orphan_files)). |
| `Cannot remove orphan files with an interval less than 24 hours. ...` | Iceberg 1.10.0 `RemoveOrphanFilesProcedure`의 24시간 안전장치(`validateInterval`)입니다. 동시 쓰기 작업의 미커밋 파일을 보호하기 위해 `older_than`이 현재보다 24시간 이내이면 `dry_run`도 거부하므로, `older_than`을 **최소 24시간보다 이전 시각**으로 지정합니다(실측·코드: [RemoveOrphanFilesProcedure.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.0/spark/v3.5/spark/src/main/java/org/apache/iceberg/spark/procedures/RemoveOrphanFilesProcedure.java), [`lab.py`](lab.py) 169행). |
| `Starting snapshot (exclusive) ... is not a parent ancestor of end snapshot ...` | `expire_snapshots`가 증분 읽기의 시작 스냅샷(`start-snapshot-id`)을 만료시켜 스냅샷 계보가 끊긴 경우입니다(`progress-trap`의 `unsafe` 단계에서는 의도된 정상 재현). 실제 운영에서는 `progress` 이전 시각까지만 만료하거나 전체 재계산 후 `progress`를 재설정합니다(실측·코드). |
| `이미 있는 데모 run-id: ... 새로 준비하려면 demo.sh --fresh를 쓰세요.` | `demo.sh`의 고정 데모 테이블이 이미 존재합니다. `./labs/session07/demo.sh --fresh`로 실행합니다(코드: [`demo.sh`](demo.sh)). |
| `ValueError: run-id: s07-로 시작하고 소문자/숫자/_/-만 사용하세요(최대 40자).` | `--run-id` 이름 규칙을 확인합니다(코드: [`lab.py`](lab.py) 54~55행). |
| `ValueError: 이 run-id의 테이블이 있어요. prepare/all은 새 run-id로 시작하세요.` | 이미 만든 `run-id`로 `prepare`나 `all`을 다시 호출한 경우입니다. 새 `run-id`를 쓰거나 `clean` 뒤에 실행합니다(코드: [`lab.py`](lab.py) 99~101행). |
| `ValueError: grow는 비어 있는 새 실험에서 한 번만 실행하세요.` | 이미 `grow`를 실행한 테이블에서 중복 실행한 경우입니다. 다음 단계인 `compact`로 넘어가거나 새 `run-id`를 사용합니다(코드: [`lab.py`](lab.py) 128~129행). |

---

## 9. 개인 프로젝트 리뷰 질문지 (핵심 6 + 6·7회차 3)

후반부 프로젝트 리뷰 시간에는 아래 **9개 질문**(스터디 공통 핵심 질문 6개 + 6·7회차 심화 질문 3개)을 기준으로 각자의 파이프라인 설계를 설명하고 피드백을 주고받습니다(설계: [`labs/README.md`](../README.md)).

- **진행 방식**: 참석 인원에 맞춰 1인당 발표·질문 시간을 똑같이 나눕니다(예: 4명이면 1인당 10분 = 설계 설명 5분 + 상호 질문·피드백 5분). 듣는 사람은 아래 질문지 중 아직 근거가 비어 있거나 운영 중 깨지기 쉬운 조건을 짚어 질문합니다.
- **제출 안내**: 수업 중 제출 절차는 없습니다. 과제 형식과 기한은 별도 공지를 따릅니다.
- **8회차 예고**: 다음 8회차에서는 오늘 점검한 증분·유지보수 설계를 **Airflow 스케줄·장애 복구·최종 운영 점검**으로 실제 연결합니다.

### 스터디 핵심 질문 6개 (1~5회차 기반, 설계)

| 번호 | 질문 | 내 프로젝트에서 구체적으로 답할 내용 |
|---|---|---|
| **① 사용자·의사결정** | **누가 어떤 판단에 쓰는 데이터인가요?** | Gold 마트를 읽는 구체적인 사용자(또는 시스템)와 그 숫자로 내리는 결정 |
| **② 신선도 목표(SLA)** | **얼마나 빨리 반영돼야 하나요?** | 원천 발생부터 Gold 조회까지 허용 지연 시간(예: 5분, 30분, 일 배치)과 그 이유 |
| **③ 변경·중복 처리** | **수정·삭제·중복 전달은 어떻게 처리하나요?** | 원천 CDC/이벤트의 중복 도착, 순서 역전(LSN·타임스탬프), 삭제(`is_deleted`·철회) 반영 규칙 |
| **④ 실패 복구 시작점** | **실패하면 어디서 다시 시작하나요?** | 체크포인트·오프셋·스냅샷 `progress`·`plan` 등 중단 지점을 식별하고 이어 달리는 기준 |
| **⑤ 정확성 근거** | **맞다는 근거는 무엇인가요?** | 모델 계약(`contract`), 데이터 테스트, 전체 재계산과의 양방향 `EXCEPT` 대조 등 검증 장치 |
| **⑥ 구조 선택과 트레이드오프** | **그 구조를 고른 이유와 감수한 비용·제약은 무엇인가요?** | 전체 교체(`table`) 대 증분(`MERGE`), 스트리밍 대 배치 선택 이유와 그로 인해 감수한 운영 복잡도·지연·리소스 비용 |

### 6·7회차 추가 질문 3개 (증분·멱등성·유지보수, 설계)

| 번호 | 질문 | 내 프로젝트에서 구체적으로 답할 내용 |
|---|---|---|
| **⑦ 증분 범위와 키 전파** | **증분 범위를 어떻게 잡고 영향 키가 어디까지 전파되나요?** | 변경을 읽는 원천 테이블, 하위→상위 마트 키 조인 경로, 과거 날짜·그룹 이동 시 이전 그룹과 빈 그룹(`DELETE`) 처리 방법 |
| **⑧ 재시도 멱등성** | **중간에 실패한 뒤 재시도해도 같은 답이 나오나요(멱등성)?** | 차액 가감 대신 기준 키 전체 재합산을 쓰는지, 여러 테이블 갱신 시 쓰기 전 `plan` 고정과 완료 후 `progress` 갱신 순서를 지키는지 |
| **⑨ 커밋·보존·정리 주기** | **내 테이블의 커밋 주기·스냅샷 보존·정리 주기는 얼마인가요?** | 스트리밍/배치 커밋 주기와 빈 커밋 제어, `write.metadata.delete-after-commit.enabled` 설정 여부, `compact` → `expire` → `orphans` → `manifests` 실행 주기와 `progress` 스냅샷 보존 하한선 |

---

## 10. 더 알아보기 (미실측 참고 자료)

이번 7회차 수업과 실습 코드는 **"작은 커밋이 쌓일 때의 파일·스냅샷·메타데이터 유지보수"**에 집중했습니다. 아래의 파티션 설계, 파일 내부 정렬(bin-pack vs sort/z-order), 읽기 쿼리 튜닝은 이번 회차에서 별도 실험이나 전후 실측을 진행하지 않았으므로(**실측 없음**), 개인 프로젝트에서 대용량 조회 성능을 더 다듬고 싶을 때 공식 문서를 참고합니다(문서·설계).

- **데이터 파일 재작성 전략 (`binpack` vs `sort` / `z-order`, 문서·실측 없음)**: `rewrite_data_files`는 기본 `binpack`(작은 파일들을 목표 크기로 빠르게 묶음) 외에도 특정 열 기준 `sort`나 다차원 `zorder` 전략으로 파일 내부 데이터를 정렬해 읽기 시 파일 건너뛰기(data skipping) 효율을 높일 수 있습니다. 단, 정렬 비용이 추가로 들므로 자주 필터링하는 열이 뚜렷할 때 측정 후 도입합니다(문서: [Spark Procedures — rewrite_data_files](https://iceberg.apache.org/docs/1.10.0/spark-procedures/#rewrite_data_files), [Iceberg Maintenance](https://iceberg.apache.org/docs/1.10.0/maintenance/)).
- **파티션 진화 (Partition Evolution, 문서·실측 없음)**: Iceberg는 기존 데이터 파일을 다시 쓰지 않고도 메타데이터만 바꿔 파티션 기준(예: 월 단위 → 일 단위)을 변경하는 파티션 진화와 숨은 파티셔닝(hidden partitioning)을 지원합니다. 파티션을 너무 잘게 쪼개면 커밋마다 파티션별로 작은 파일이 더 많이 생기므로 하루 데이터 크기와 조회 조건을 함께 보고 결정합니다(문서: [Iceberg Partitioning](https://iceberg.apache.org/docs/1.10.0/partitioning/), [Spark DDL — ALTER TABLE](https://iceberg.apache.org/docs/1.10.0/spark-ddl/#alter-table)).
- **메타데이터 테이블을 활용한 쿼리·파일 상태 점검 (문서·실측 없음)**: `.files`, `.manifests`, `.partitions` 메타데이터 테이블에서 파티션별 파일 수와 열별 `lower_bounds`·`upper_bounds` 통계를 조회하면 특정 파티션에 작은 파일이 몰려 있는지 확인할 수 있습니다(문서: [Spark Queries — Inspecting tables](https://iceberg.apache.org/docs/1.10.0/spark-queries/#inspecting-tables)).
