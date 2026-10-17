# 맥미니 운영 방법

## 이번 강사 환경

- 접속: `ssh mini`
- 코드: `~/bootcamp/cdc-lakehouse-study`
- 실행: Lima `cdc-lab`, Linux ARM64, CPU 6개 / 메모리 10GiB / 디스크 40GiB
- `.runtime/host.env`가 전용 Docker CLI와 소켓을 지정한다. 다른 Docker context는 바꾸지 않는다.
- 기존 환경이 없는 경우 공식 Lima `template:docker`로 VM을 만들고 Docker CLI를 연결한다. 버전과 설치 경로는 강사 환경에 맞춰 조정한다.

```bash
limactl start --name=cdc-lab --vm-type=vz --cpus=6 --memory=10 --disk=40 \
  --mount-only="$HOME/bootcamp:w" --tty=false template:docker
export DOCKER_HOST="unix://$HOME/.lima/cdc-lab/sock/docker.sock"
```

저장소가 VM 안에서도 같은 절대 경로로 마운트되어야 한다. 외부 공개용 서버 구성은 포함하지 않는다.

## 시작·중지

```bash
cd ~/bootcamp/cdc-lakehouse-study
./scripts/bootstrap.sh --build
./scripts/stop.sh
./scripts/bootstrap.sh --no-build
```

첫 설치·이미지 변경에는 `--build`, 준비된 환경을 수업 당일 기동할 때는 `--no-build`를 사용한다. `--no-build`는 이미지 빌드·pull 없이 로컬 이미지만 사용하고, 필요한 이미지가 없으면 중단한다. 옵션을 생략하면 기존처럼 빌드한다.

`bootstrap.sh`는 Kafka 토픽, Debezium 커넥터, Iceberg 테이블과 최초 마트를 준비한다. 기존 RUNNING 작업에는 중복 제출하지 않는다. `stop.sh`는 savepoint를 남겨 종료하고 다음 `bootstrap.sh`에서 복원한다. 예고 없이 JobManager까지 사라진 경우에는 재생을 막고 명시적인 복구를 요구한다.

**`down -v`는 모든 실습 데이터를 지운다.** 일반 종료에는 사용하지 않는다. 원천·카탈로그·Kafka·MinIO 중 일부 볼륨만 초기화하면 서로 맞지 않는 상태가 되므로 독립적인 프로젝트 이름으로 새 환경을 만드는 편이 안전하다.

## UI와 SQL 접속

노트북 터미널에서 실행하고 접속 중에는 터널을 유지한다.

```bash
ssh -N -L 28081:127.0.0.1:28081 -L 28080:127.0.0.1:28080 \
  -L 29001:127.0.0.1:29001 -L 24040:127.0.0.1:24040 mini
```

| 주소 | 용도 |
|---|---|
| `http://localhost:28081` | Flink 작업·체크포인트 |
| `http://localhost:28080` | Airflow — orchestration 프로필 실행 시 |
| `http://localhost:29001` | MinIO — 로그인 값은 로컬 `.env` |
| `http://localhost:24040` | Spark 작업 |

원천 DB는 맥미니의 loopback 25432, Spark Thrift는 21000, Connect는 28083이다. 컨테이너끼리는 서비스 이름으로 통신한다. 학생별 동시에 실행하는 다중 테넌트 서버가 아니며, 여러 프로젝트를 올릴 때는 포트·리소스·스토리지 격리를 먼저 설계한다.

```bash
./scripts/lab.sh exec -T source-db psql -U source -d app
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql \
  'SELECT * FROM lake.gold.mart_daily_settlement'
```

## Airflow

```bash
./scripts/lab.sh --profile orchestration up -d airflow
./scripts/lab.sh exec -T airflow airflow dags list-import-errors
./scripts/lab.sh exec -T airflow airflow dags unpause lakehouse_refresh
./scripts/lab.sh exec -T airflow airflow dags trigger lakehouse_refresh
```

최초 생성은 paused 상태다. 초기 검증이 끝난 뒤 unpause하면 5분마다 Silver → dbt build를 실행한다. UI 계정은 `admin`, 초기 비밀번호는 Airflow 컨테이너의 `/opt/airflow/standalone_admin_password.txt`에 생성된다. 공유 저장소에 비밀번호를 복사하지 않는다.

Airflow는 내부 runner API에 고정된 작업만 요청한다. Docker 소켓을 넘기지 않는다. API는 호스트 포트를 열지 않으며 신뢰된 수업용 Compose 네트워크를 전제로 한다. runner가 재시작하면 API 실행 이력이 사라지고 Airflow 작업은 실패할 수 있다. 로그를 확인한 뒤 멱등적으로 다시 실행한다.

## 점검 순서

1. 원천 DB에서 변경이 실제 커밋됐는지 확인한다.
2. Connect `/connectors/source-postgres/status`의 커넥터와 태스크 상태를 확인한다.
3. Kafka 토픽에서 이벤트를 확인한다.
4. Flink 체크포인트와 로그를 확인한다.
5. Bronze SQL에서 원문을 조회한다. RUNNING 또는 checkpoint 성공만으로 적재 완료를 판단하지 않는다.
6. Silver 최신 버전·삭제 표시, Gold 기대값, dbt 테스트 결과를 확인한다.

## 복구 경계

`recovery.sh`는 JobManager를 유지한 채 TaskManager를 중지·시작하는 실습이다. Flink가 보관한 checkpoint와 소스 오프셋으로 재개하는지 검증한다. 계획된 종료는 `stop.sh` → `bootstrap.sh`로 savepoint 복원을 사용한다. 수집 설정을 변경한 뒤에는 `ingest-restart.sh`로 같은 작업을 savepoint에서 재시작한다. 작업의 SQL 구조가 바뀌면 상태 호환성을 별도로 확인해야 한다. 임의로 checkpoint·snapshot·객체 파일을 삭제하지 않는다.

브론즈 전체 재조회와 버전 기반 MERGE 덕분에 같은 입력의 재처리는 결과를 유지하지만, 이는 모든 장애에서 end-to-end exactly-once를 보장한다는 뜻은 아니다. 원천 WAL 보존, Kafka 보관 기간, 카탈로그와 객체 데이터의 일관성이 복구 가능 범위를 제한한다.
