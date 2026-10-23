# 개인 실습 환경 운영 방법

## 실행 환경

Linux ARM 컨테이너를 실행할 수 있는 Docker 환경을 사용한다. 저장소를 clone한 뒤 저장소 루트에서 아래 명령을 실행한다. 강사 서버에 접속하거나 강사 전용 VM·로컬 설정을 복사하지 않는다. 최초 환경 요건과 실행 순서는 [README](../README.md)를 함께 본다.

수업 중에는 강사의 맥미니 화면을 함께 보고, 수업 후 복습에서는 각자의 환경을 사용한다. 아래 접속 주소는 개인 환경의 localhost 기준이다.

## 시작·중지

```bash
cp .env.example .env
./scripts/bootstrap.sh --build
./scripts/stop.sh
./scripts/bootstrap.sh --no-build
```

`.env` 복사는 최초 구성에서 한 번만 진행하며, 기존 설정 파일이 있으면 덮어쓰지 않는다. 첫 설치·이미지 변경에는 `--build`, 이미지가 준비된 환경을 다시 시작할 때는 `--no-build`를 사용한다. `--no-build`는 이미지 빌드·pull 없이 로컬 이미지만 사용하고, 필요한 이미지가 없으면 중단한다. 옵션을 생략하면 기본으로 빌드한다.

`bootstrap.sh`는 Kafka 토픽, Debezium 커넥터, Iceberg 테이블과 최초 마트를 준비한다. 기존 RUNNING 작업에는 중복 제출하지 않는다. `stop.sh`는 savepoint를 남겨 종료하고 다음 `bootstrap.sh`에서 복원한다. 예고 없이 JobManager까지 사라진 경우에는 재생을 막고 명시적인 복구를 요구한다.

환경 준비에 Silver·Gold 작업이 포함되더라도 1–2회차 복습에 해당 모델 구현을 추가하는 것은 아니다. 공통 예제 실행 범위는 각 회차 학생 가이드를 따른다.

**`down -v`는 모든 실습 데이터를 지운다.** 일반 종료에는 사용하지 않는다. 원천·카탈로그·Kafka·MinIO 중 일부 볼륨만 초기화하면 서로 맞지 않는 상태가 되므로 독립적인 프로젝트 이름으로 새 환경을 만드는 편이 안전하다.

## MinIO 이미지 다운로드에서 `unauthorized`가 발생할 때

기존 `quay.io/minio/minio`, `quay.io/minio/mc` 공개 이미지에 접근할 수 없어 신규 설치가 실패할 수 있다. 현재 `compose.yaml`은 MinIO의 커뮤니티 포크인 [SILO](https://github.com/pgsty/silo/releases/tag/RELEASE.2026-10-18T00-00-00Z)와 [mc](https://github.com/pgsty/mc/releases/tag/RELEASE.2026-10-18T00-00-00Z)를 사용하며, ARM64·AMD64 멀티 아키텍처 이미지 해시까지 고정한다.

```bash
git pull --ff-only
./scripts/bootstrap.sh --build
```

이 경우 Docker 로그인 정보 초기화, `.env` 덮어쓰기, `down -v`는 필요하지 않다. 서비스 이름과 내부 S3 주소 `http://minio:9000`, `warehouse` 버킷은 유지된다. 기존 환경의 저장소 엔진도 바뀌므로 이미 데이터가 있는 환경은 `./scripts/stop.sh`로 계획된 종료 후 백업을 보관하고 업데이트한다. 신규 설치 검증이 기존 데이터의 업그레이드·다운그레이드 검증을 대신하지는 않는다.

## UI와 SQL 접속

Docker 환경을 실행한 컴퓨터의 브라우저에서 아래 주소를 연다. Compose는 호스트 포트를 `127.0.0.1`에 바인딩하므로 같은 컴퓨터에서 접속한다.

| 주소 | 용도 |
|---|---|
| `http://localhost:28081` | Flink 작업·체크포인트 |
| `http://localhost:28080` | Airflow — orchestration 프로필 실행 시 |
| `http://localhost:29001` | MinIO — 로그인 값은 로컬 `.env` |
| `http://localhost:24040` | Spark 작업 |

원천 DB는 localhost 25432, Spark Thrift는 21000, Connect는 28083이다. 컨테이너끼리는 서비스 이름과 내부 포트로 통신하므로 호스트 접속 포트와 구분한다. 여러 프로젝트를 함께 실행할 때는 포트·리소스·스토리지 충돌 여부를 먼저 확인한다.

```bash
./scripts/lab.sh exec -T source-db psql -U source -d app
./scripts/lab.sh exec -T runner python scripts/pipeline.py --sql \
  'SELECT * FROM lake.gold.mart_daily_settlement'
```

## 후속 회차 — Airflow

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

## 후속 회차 — 복구 실험과 경계

`recovery.sh`는 JobManager를 유지한 채 TaskManager를 중지·시작하는 실습이다. Flink가 보관한 checkpoint와 소스 오프셋으로 재개하는지 검증한다. 계획된 종료는 `stop.sh` → `bootstrap.sh`로 savepoint 복원을 사용한다. 수집 설정을 변경한 뒤에는 `ingest-restart.sh`로 같은 작업을 savepoint에서 재시작한다. 작업의 SQL 구조가 바뀌면 상태 호환성을 별도로 확인해야 한다. 임의로 checkpoint·snapshot·객체 파일을 삭제하지 않는다.

브론즈 전체 재조회와 버전 기반 MERGE 덕분에 같은 입력의 재처리는 결과를 유지하지만, 이는 모든 장애에서 end-to-end exactly-once를 보장한다는 뜻은 아니다. 원천 WAL 보존, Kafka 보관 기간, 카탈로그와 객체 데이터의 일관성이 복구 가능 범위를 제한한다.
