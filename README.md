# CDC부터 분석 마트까지

부트스트랩 정규 과정을 바탕으로 진행하는 8회 실무 프로젝트 스터디의 강사 참고 구현과 실습 가이드입니다. PostgreSQL의 변경을 Iceberg에 실시간으로 쌓고, Spark SQL과 dbt로 분석 마트를 만듭니다. 실행 환경은 **Mac mini**, 코드 공유는 **GitHub**입니다.

[1회차 수업 자료](labs/session01/README.md) · [전체 아키텍처](docs/architecture.md) · [8회 실습](labs/README.md) · [개인 프로젝트 설계서](docs/project-template.md) · [운영 방법](docs/operations.md) · [검증 결과](docs/verification.md)

![데이터 흐름](docs/diagrams/01-data-flow.png)

[저장·메타데이터 관계와 실행 환경 보기](docs/diagrams/README.md)

```text
PostgreSQL → Debezium → Kafka → Flink → Iceberg Bronze
                                             ↓
                                  Spark SQL → Iceberg Silver
                                             ↓
                              dbt + Spark → Iceberg Gold
                                  ↑ Airflow가 주기 실행
```

주문·결제·환불은 동작을 이해하기 위한 강사 예시입니다. 1–2회차에는 공통 예제로 수집 흐름을 익히고, 3회차에 개인 도메인·원천 테이블 설계를 확정한 뒤 지표와 허용 지연을 구체화합니다. 1회차 학생은 서버 실행 없이 공통 예제 SQL·이벤트 예측과 수집 대조 기록을 작성합니다. 발표는 8회 스터디 이후 별도 일정에 진행합니다.

## 빠른 실행 — 맥미니에서

Linux ARM 컨테이너를 실행할 Docker 환경이 필요합니다. 이 수업의 맥미니는 전용 Lima VM `cdc-lab`을 사용합니다. 기존 Docker 환경이라면 `.runtime/host.env` 없이 그대로 실행합니다.

```bash
cp .env.example .env
./scripts/bootstrap.sh
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql \
  'SELECT * FROM lake.gold.mart_daily_settlement'
```

이미지 준비 후 수업 당일에는 `./scripts/bootstrap.sh --no-build`로 기동해 빌드와 이미지 다운로드를 생략합니다.

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
| `compose.yaml`, `docker/` | 맥미니 실행 환경 |
| `sql/` | 예제 원천 DB |
| `flink/` | Kafka 원문을 Iceberg에 저장하는 SQL |
| `scripts/pipeline.py` | 변경 순서 해석과 Silver MERGE |
| `dbt/` | 분석 모델과 데이터 테스트 |
| `airflow/dags/` | 주기 실행 |
| `labs/` | 회차별 실습과 제출 기준 |
| `docs/` | 설계·운영·검증 기록 |

## 참고 문서

- [PostgreSQL logical decoding](https://www.postgresql.org/docs/17/logicaldecoding.html)
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html)
- [Iceberg Flink writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/)
- [Iceberg Spark writes](https://iceberg.apache.org/docs/1.10.0/spark-writes/)
- [dbt Spark 연결](https://docs.getdbt.com/docs/local/connect-data-platform/spark-setup)
- [Lima Docker VM](https://lima-vm.io/docs/examples/containers/docker/)
