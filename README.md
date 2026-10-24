# CDC부터 분석 마트까지

부트스트랩 정규 과정을 바탕으로 진행하는 8회 실무 프로젝트 스터디의 공통 예제와 학생용 실습 자료입니다. PostgreSQL의 변경을 Iceberg에 실시간으로 쌓고, Spark SQL과 dbt로 분석 마트를 만듭니다. 수업에서는 강사의 **Mac mini 화면**을 함께 보고, 복습에서는 각자의 **Docker 환경**에서 공통 예제를 실행합니다.

[1회차 수업 자료](labs/session01/README.md) · [2회차 수업 자료](labs/session02/README.md) · [3회차 Flink·Bronze와 개인 설계](labs/session03/README.md) · [개인 공통 실행 안내](docs/self-study.md) · [전체 아키텍처](docs/architecture.md) · [8회 실습](labs/README.md) · [개인 프로젝트 설계서](docs/project-template.md) · [개인 환경 운영 안내](docs/operations.md)

![데이터 흐름](docs/diagrams/01-data-flow.png)

[저장·메타데이터 관계와 실행 환경 보기](docs/diagrams/README.md)

```text
PostgreSQL → Debezium → Kafka → Flink → Iceberg Bronze
                                             ↓
                                  Spark SQL → Iceberg Silver
                                             ↓
                              dbt + Spark → Iceberg Gold
                                  ↑ Airflow를 활성화하면 주기 실행
```

주문·결제·환불은 동작을 이해하기 위한 공통 예제입니다. 1–2회차에는 공통 예제로 수집 흐름을 익히고, 3회차에 개인 도메인·원천 테이블 설계를 확정한 뒤 지표와 허용 지연을 구체화합니다. 1–2회차 수업 중에는 학생이 서버를 기동하지 않고 student-worksheet.sql로 코드 읽기·SELECT 작성·이벤트 예측을 진행하며 실제 수집 결과와 대조합니다. 수업 중 submission.md 작성이나 파일 제출은 진행하지 않으며 관찰 메모는 선택입니다. 수업 후에는 개인 환경을 구성해 공통 예제를 실행하고 복습 결과를 정리합니다. 발표는 8회 스터디 이후 별도 일정에 진행합니다.

3회차는 **Flink → Bronze 적재 심화, 개인 도메인·테이블 설계 확정**입니다. 요구사항과 선택 이유를 먼저 정하고, 실제 Flink SQL의 raw 입력·전달 메타데이터·tombstone 필터·append를 읽습니다. 짧은 강사 실험으로 저장 결과와 조회 경계를 확인한 뒤, [설계 학습지](labs/session03/design-worksheet.md)에 도메인·원천 관계·변경 의미·토픽·Bronze 보존 정보를 확정합니다. 이후 회차도 파이프라인 설계를 중심으로 진행하며 공통 예제 전체를 직접 확인하려면 [개인 실행 안내](docs/self-study.md)를 사용합니다.

## 빠른 실행 — 개인 Docker 환경에서

Linux ARM 컨테이너를 실행할 Docker 환경이 필요합니다. 저장소를 clone한 뒤 루트 디렉터리에서 실행합니다. 강사 서버에 접속하거나 강사 전용 환경 설정을 복사할 필요는 없습니다.

먼저 실행 계획을 읽고, 처음 설치하는 환경이면 `first`를 실행합니다.

```bash
./scripts/self-study.sh --plan
./scripts/self-study.sh --environment first
```

필요한 서비스가 이미 실행 중이면 `--environment reuse`(기본값), 준비해 둔 환경이 중지돼 있으면 `--environment start`를 선택합니다. 세 환경 모드를 차례로 실행하지 않습니다. 환경 요건과 결과 읽는 방법은 [개인 공통 실행 안내](docs/self-study.md)를 따릅니다.

기본 실행은 새 키로 원천·Kafka·Bronze·실제 Parquet를 대조하고 단계별 보고서를 남깁니다. 새 사건을 Silver·Gold까지 갱신하려면 선택 옵션 `--with-marts`를 추가합니다.

S3 저장소는 MinIO에서 이어진 커뮤니티 포크 [SILO](https://github.com/pgsty/silo)를 사용합니다. 공개 다운로드가 중단된 `quay.io/minio/*` 대신 `pgsty/silo`와 `pgsty/mc`의 검증된 버전·이미지 해시를 고정했습니다. 실습의 서비스 이름(`minio`), 접속 주소, 계정 설정과 `warehouse` 버킷은 동일합니다. 이전 설치에서 `unauthorized`가 발생했다면 저장소를 업데이트하고 `./scripts/bootstrap.sh`를 다시 실행합니다. Docker 설정 파일 초기화나 데이터 볼륨 삭제는 필요하지 않습니다.

bootstrap은 전체 예제 환경을 준비하며 Silver·Gold/dbt 작업도 실행합니다. 1–2회차의 학습 범위는 원천 변경 수집과 Bronze 확인이며, Silver·Gold 모델 구현은 후속 회차에서 다룹니다.

초기 기준값은 2026-10-01 주문 한 건, 결제 100,000원입니다. 마트의 `net_paid`는 **주문일 기준 결제 성공액 − 환불 성공액**이며 회계상 매출을 의미하지 않습니다.

```bash
# 수정·삭제가 분석 결과에 반영되는지 확인
./scripts/verify.sh
# 컨테이너 상태와 로그
./scripts/lab.sh ps
./scripts/lab.sh logs --tail 100 taskmanager
# 데이터는 유지하고 중지
./scripts/stop.sh
```

`.env.example`의 값은 격리된 수업용 기본값입니다. 실제 비밀번호, AWS 키, 회사 데이터는 커밋하지 않습니다. 이미지와 jar 버전은 Dockerfile에 고정되어 있으며 최초 빌드에 네트워크가 필요합니다.

## 디렉터리

| 경로 | 내용 |
|---|---|
| `compose.yaml`, `docker/` | 공통 Docker 실행 환경 |
| `sql/` | 예제 원천 DB |
| `flink/` | Kafka 원문을 Iceberg에 저장하는 SQL |
| `scripts/pipeline.py` | 변경 순서 해석과 Silver MERGE |
| `dbt/` | 분석 모델과 데이터 테스트 |
| `airflow/dags/` | 주기 실행 |
| `labs/` | 회차별 실습과 제출 기준 |
| `docs/` | 아키텍처·개인 설계·환경 운영 안내 |

## 참고 문서

- [PostgreSQL logical decoding](https://www.postgresql.org/docs/17/logicaldecoding.html)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html)
- [Iceberg Flink writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/)
- [Iceberg Spark writes](https://iceberg.apache.org/docs/1.10.0/spark-writes/)
- [dbt Spark 연결](https://docs.getdbt.com/docs/local/connect-data-platform/spark-setup)
- [Lima Docker VM](https://lima-vm.io/docs/examples/containers/docker/)
