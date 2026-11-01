# Lakehouse Explorer

Spark SQL 결과에서 Iceberg 메타데이터와 MinIO 파일까지 따라가 보는 조회 전용 화면입니다. 기본 실습 환경을 먼저 실행한 뒤 선택해서 켭니다.

## 실행과 중지

저장소 루트에서 실행합니다. Spark Thrift와 MinIO가 준비되어 있어야 합니다. 첫 실행에는 이미지와 Python 패키지를 내려받을 네트워크가 필요합니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml up -d --build --no-deps explorer
```

같은 컴퓨터에서 <http://localhost:28090>을 엽니다. 상단의 Spark Thrift와 MinIO 상태, 마지막 확인 시각을 확인합니다. 연결 확인 버튼으로 다시 확인할 수 있습니다. 다른 서비스는 이 명령으로 시작하거나 재생성하지 않습니다.

```bash
./scripts/lab.sh -f compose.yaml -f compose.observers.yaml rm -sf explorer
```

Explorer만 중지하고 제거합니다. 실습 데이터는 그대로 유지됩니다.

## 화면 사용

1. **SQL**에서 예제를 고른 뒤 실행합니다. Ctrl/Cmd+Enter로도 실행할 수 있습니다. 결과는 50, 200, 500행 중 선택해서 봅니다. 최근 실행 20개는 사용 중인 브라우저에 저장됩니다. 브라우저 저장이 차단되어도 조회는 동작합니다.
2. **테이블**에서 네임스페이스와 테이블을 고릅니다. 최근 스냅샷 20개, 현재 데이터 파일, 메타데이터 파일 이력을 볼 수 있습니다. 경로를 누르면 파일 화면으로 이동합니다.
3. **파일**에서 폴더와 경로를 따라갑니다. 목록은 폴더와 파일을 합해 최대 1000개씩 봅니다. 다음 페이지가 있으면 아래 버튼이 나타납니다. JSON은 항목을 펼쳐 보고, Avro는 앞 200개 레코드, Parquet는 스키마·행 그룹 통계·앞 50행을 봅니다.

화면 공유할 때는 상단의 글자 크게 버튼을 누릅니다. 외부 CDN이나 웹 폰트를 사용하지 않습니다. 예제 테이블이 아직 없다면 Spark 오류가 표시됩니다.

## 실행할 수 있는 SQL

`SELECT`, `WITH`, `SHOW`, `DESCRIBE`, `DESC`, `EXPLAIN`, `VALUES`, `TABLE`로 시작하는 조회 문장 한 개만 허용합니다. 끝의 세미콜론 하나와 주석, 문자열은 사용할 수 있습니다. `SHOW CREATE TABLE`도 허용합니다.

`INSERT`, `UPDATE`, `DELETE`, `MERGE`, `CREATE`, `DROP`, `ALTER`, `TRUNCATE`, `CALL`, `REFRESH`, `CACHE`, `UNCACHE`, `CLEAR`, `SET`, `RESET`, `USE`, `ADD`, `LOAD`, `MSCK`, `REPAIR`, `ANALYZE`, `GRANT`, `REVOKE`, `OPTIMIZE`, `VACUUM`, `COPY`, `EXPORT`, `IMPORT`, `LOCK`, `UNLOCK`, `EXECUTE`는 리터럴 밖에 있으면 거부합니다. `EXPLAIN INSERT`나 `WITH ... INSERT`도 실행할 수 없습니다. 열 이름이 금지 단어와 같으면 거부될 수 있습니다. `date_add`, `is_deleted`, `replace()`는 사용할 수 있습니다.

Spark Thrift는 dbt·파이프라인과 세션을 공유합니다. Explorer는 세션 설정, 현재 네임스페이스, 임시 뷰를 바꾸지 않으며 PyHive가 연결 때 자동으로 보내는 `USE default`도 보내지 않습니다. 테이블 조회에는 `lake.silver.refunds`처럼 전체 이름을 사용하는 편이 좋습니다.

## 제한 사항

- SQL은 20,000자까지 입력할 수 있습니다. 서버 전체에서 조회는 두 개까지 동시에 실행하고, 그 밖의 조회는 최대 20초까지 기다렸다가 실행합니다. 연결 상태 확인도 조회 한 개를 사용합니다.
- 기본 제한 시간은 60초이며 초과하면 취소를 시도합니다. 취소와 연결 종료는 통신 상태에 따라 늦어질 수 있습니다. 정리가 끝나기 전에는 해당 실행 자리를 재사용하지 않습니다.
- 결과만 앞부분으로 자릅니다. SQL 자체를 수정하지 않으므로 큰 정렬·집계는 Spark 자원을 사용할 수 있습니다. dbt·파이프라인이 실행 중이면 잠시 기다려 주세요.
- 테이블 정보는 항목별 최대 500행입니다. 그보다 많으면 SQL 탭에서 조건을 넣어 조회합니다. 테이블 정보의 세 쿼리 사이에 다른 작업이 커밋되면 시점이 다를 수 있습니다.
- `s3://warehouse/<key>`와 `s3a://warehouse/<key>`만 엽니다. 다른 버킷, HTTP 주소, 로컬 파일 경로는 거부합니다.
- 파일은 64MB까지 미리 봅니다. 더 크면 크기와 수정 시각만 보여 줍니다. 다른 형식은 앞 4KB를 텍스트나 hex로 봅니다. 파일 미리보기는 한 번에 하나씩 읽습니다. 압축률이 높거나 구조가 복잡한 파일은 메모리를 많이 사용할 수 있습니다.
- 접속은 Docker 호스트의 `127.0.0.1:28090`으로만 열립니다. 수업용 로컬 환경에서 사용합니다.

## 개발 확인

```bash
python -m unittest discover -s tools/explorer/tests
```

SQL 검사·경로 변환·실행 제한·스토리지 경계 테스트는 외부 패키지 없이 실행됩니다. API 테스트는 FastAPI가 설치되어 있을 때 실행됩니다. 실제 Spark·MinIO 연결과 파일 파서는 전체 의존성을 설치한 실행 환경에서 따로 확인해야 합니다.

Python 3.11 기반 이미지와 Python 의존성 버전은 고정했습니다. ARM64 wheel은 PyPI 배포 목록에서 확인했습니다.

- [pyarrow 18.1.0](https://pypi.org/project/pyarrow/18.1.0/): `pyarrow-18.1.0-cp311-cp311-manylinux_2_28_aarch64.whl`
- [fastavro 1.10.0](https://pypi.org/project/fastavro/1.10.0/): `fastavro-1.10.0-cp311-cp311-manylinux_2_17_aarch64.manylinux2014_aarch64.whl`

이미지 빌드에서는 두 패키지의 wheel만 허용합니다. wheel 배포 확인과 실제 ARM64 이미지 설치·실행 확인은 별도입니다.
