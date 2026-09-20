# 맥미니 실행 검증 — 2026-09-20

이 기록은 구성 파일 검토가 아니라 `ssh mini`로 접속한 Mac mini의 실제 컨테이너에서 수행한 결과다. 테스트 데이터는 합성 주문·결제·환불이며 원천부터 Gold까지 조회했다.

## 통과한 시나리오

| 입력 | 기대 순결제액 | 실제 순결제액 |
|---|---:|---:|
| 신규 결제 100,000원 | 100,000 | 100,000 |
| 환불 20,000원 입력 | 80,000 | 80,000 |
| 같은 환불 이벤트 재전달 | 80,000 | 80,000 |
| 환불을 30,000원으로 수정 | 70,000 | 70,000 |
| 환불 행 삭제 | 100,000 | 100,000 |
| 삭제 뒤 과거 환불 수정 이벤트 재전달 | 100,000 | 100,000 |

[변경 검증 원본](evidence/change-cases.json). 같은 이벤트를 Kafka에 다시 발행해 새로운 오프셋을 부여했으므로, 단순히 같은 함수만 두 번 호출한 검증이 아니다. 삭제 표시도 Silver에 남아 있음을 확인했다.

- **TaskManager 장애 복구:** 작업자를 멈춘 동안 결제를 50,000원에서 70,000원으로 수정했다. checkpoint 31에서 복원되었고, Gold는 70,000원이었다. 동일 topic/partition/offset의 Bronze 중복 행은 0건이었다. [복구 기록](evidence/worker-recovery.json)
- **계획된 수집 재시작:** savepoint를 생성하며 작업을 중지하고 새 SQL 설정으로 복원했다. 복원 후 위 변경 시나리오를 다시 통과했다.
- **dbt:** 테이블 모델 2개 생성, 데이터 테스트 6개 통과. [런타임 증거](evidence/runtime-evidence.json)
- **Airflow:** DAG import 오류 없음. 수동 실행과 정기 실행 모두 success. [실행 기록](evidence/airflow-runs.json)

## 지연 측정의 의미

최종 변경 검증에서 Bronze 조회까지 약 4.3–6.7초, 즉시 Spark SQL·dbt를 실행해 Gold를 검증하기까지 약 8.2–10.8초가 관측됐다. 동일 컨테이너의 monotonic clock과 약 2초 간격 조회로 측정한 소량 데이터 결과다. 재전달 시나리오는 DB 변경 대신 Kafka 재발행부터 측정한다.

**위 Gold 수치는 Airflow 5분 주기 대기 시간을 포함하지 않는다.** 정기 실행에서는 다음 스케줄을 기다리는 시간이 추가된다. 수업의 목표는 이 차이를 직접 측정하고 프로젝트 요구에 맞게 주기를 정하는 것이다. 이 결과를 운영 SLA 또는 처리량 성능으로 해석하지 않는다.

## 실제 사용한 버전

PostgreSQL 17.6 / Debezium 3.2.4.Final / Kafka 3.9.1 / Flink 1.20.4 / Iceberg 1.10.0 / Spark 3.5.6 / dbt-spark 1.9.3 / dbt-core 1.12.5 / Airflow 2.10.5. Python 의존성은 실행 환경에서 추출한 `docker/runner/requirements.lock`으로 고정했다.

## 준비 중 발견하고 수정한 것

- MinIO Docker Hub 이미지 다운로드가 실패해 동일 릴리스의 공식 Quay 이미지를 사용했다.
- Flink 카탈로그는 `catalog-type=jdbc` 대신 `catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog`로 설정했다.
- 외부 쓰기 결과가 Spark 조회에 늦게 보이는 문제를 막도록 Iceberg 카탈로그 캐시를 비활성화했다.
- 단순 컨테이너 종료 대신 savepoint를 남기는 종료 스크립트를 제공했다. 복구 지점 없는 재제출은 자동으로 막는다.

## 이 검증에 포함하지 않은 것

대용량 처리량, 다중 노드 HA, JobManager 비정상 소실, 원천 WAL 또는 Kafka 보관 기간 초과, 스키마 변경 자동 전파, 다중 파티션·다중 원천 버전 비교, 테이블 간 트랜잭션 일관성은 별도 과제다. Silver는 전체 Bronze를 조회하고 Gold는 전체 재생성하며, 증분 최적화는 6회차 구현 과제로 구분했다.
