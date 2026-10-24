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

`orders`·`payments`·`refunds`는 1–3회차에 함께 다루는 공통 예제입니다. 1회차에는 세 테이블의 전체 구조와 환경을 파악하고, 2회차에는 PostgreSQL → Debezium → Kafka, 3회차에는 Flink → Iceberg Bronze 적재와 지연을 확인합니다. 결제 100,000원에 환불 20,000원을 입력하고 30,000원으로 정정한 뒤 환불 행을 삭제합니다. 이 예제에서 삭제는 **환불 철회**를 뜻합니다. c/u/d 이력과 checkpoint·commit을 직접 대조한 뒤 3회차 말에 각자의 도메인·테이블 설계를 확정합니다.

수업에서는 강사 공유 화면을 보며 결과를 예측하고 SQL을 작성·읽으며 설계와 수집 결과를 대조합니다. 1–2회차는 `student-worksheet.sql`, 3회차는 [설계 학습지](labs/session03/design-worksheet.md)를 사용합니다. 수업 중 `submission.md` 작성이나 파일 제출은 진행하지 않습니다. 수업 후에는 [개인 공통 예제 실행 안내](docs/self-study.md)에 따라 각자의 Docker 환경에서 같은 흐름을 실행합니다.

3회차는 **Flink → Bronze 적재 심화, 개인 도메인·테이블 설계 확정**입니다. Flink·Iceberg의 정의와 대안을 비교하고, 공통 예제의 raw SQL과 writer·checkpoint·테이블 commit을 연결합니다. MinIO의 실제 파일에서 Catalog 참조·snapshot·같은 키 조회를 확인하고 운영 지표로 다음 조사 지점을 판단합니다. 개인 설계 30분과 리뷰 12분에는 도메인·원천 관계·변경/삭제 정책·토픽·Bronze 보존 정보와 후속 조인 키를 확정합니다. 공통 주문 예제로 개인 프로젝트의 주제를 통일하지 않습니다.

**각자의 프로젝트**로 4회차에는 Spark Silver 중복 제거·MERGE, 5회차에는 dbt Gold·테스트, 6회차에는 수정·삭제·소급 정정을 구현합니다. 7회차에는 Iceberg rewrite_data_files·파티션·쿼리를 튜닝하고, 8회차에는 Airflow 연동·장애 복구·최종 리뷰를 진행합니다. 발표는 8회 스터디 이후 별도 일정입니다. [첫날 로드맵과 3회차 흐름 보기](labs/session03/student-guide.md).

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

bootstrap은 전체 예제 환경을 준비하며 Silver·Gold/dbt 작업도 실행합니다. 1–3회차의 수집·적재 학습 범위는 원천 변경부터 Bronze까지이며, Silver·Gold 모델 구현은 후속 회차에서 다룹니다.

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
