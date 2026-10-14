# 1회차 리허설 기록 — 2026-10-14

맥미니의 실제 PostgreSQL, Kafka/Connect, Flink, Iceberg, Spark SQL·dbt에서 강사용 실행 순서의 핵심 세 단계를 수행했다. 시연 ID는 `s01-rehearsal-20261014a`이며, 각각의 결과를 JSON으로 보관했다.

| 단계 | 원천 순결제액 | Gold 순결제액 | 확인 |
|---|---:|---:|---|
| 결제 입력 후 갱신 | 100,000 | 100,000 | [기록](evidence/payment.json) |
| 환불 입력 후 갱신 전 | 80,000 | 100,000 | [중간 상태](evidence/refund-before-refresh.json) |
| 환불 입력 후 갱신 완료 | 80,000 | 80,000 | [기록](evidence/refund-after-refresh.json) |
| 같은 환불 금액 정정 후 갱신 | 70,000 | 70,000 | [기록](evidence/correction.json) |

[Bronze 원문 관찰](evidence/bronze-events.json)에서 주문 c, 결제 c, 환불 c, 환불 u를 확인했다. 환불 u는 before_amount=20000 / after_amount=30000이었다. Silver에는 해당 환불 한 행이 amount=30000, is_deleted=false로 남았다.

원천 집계 답안 SQL도 실행해 paid=100000 / refunded=30000 / net_paid=70000을 확인했다. 각 refresh에서 dbt 모델 2개와 데이터 테스트 6개가 통과했다. [런타임 증거](evidence/runtime.json)

## 실행 준비 상태

[preflight 결과](evidence/preflight.json)는 ready=true, VM 여유 공간 약 3.48GiB였다. 이 값은 리허설 시작 시점이며 수업 시작 전 다시 확인한다. 2시간 연속 부하나 학생별 서버 동시 접속을 검증한 기록은 아니다. 수업 방식은 강사 시연과 학생의 로컬 SQL·설계서 작성이다.

첫 점검에서는 Spark OOM 종료, 수집 작업 부재, VM 디스크 부족을 발견했다. 기존 checkpoint 71106에서 수집을 복구하고 리허설했다. 쌓인 Iceberg 저장 공간과 사용하지 않는 이미지가 공간 부족에 영향을 줬다. Docker 로그도 별도 관리가 필요했다.

- 종료된 Spark의 로그는 맥미니 로컬 `.runtime/archive/`에 압축 보관했다.
- 사용하지 않는 이미지와 1회차에 필요 없는 Airflow 실행 리소스를 정리했다. Airflow 데이터 볼륨은 유지되며 이후 회차에서 이미지를 다시 받아 실행할 수 있다.
- compose에 로그 파일 크기·개수 제한을 적용했다.
- Spark UI가 보관하는 실행·작업·스테이지 개수도 제한했다. 이것만으로 모든 OOM이 해결된다고 판단하지 않는다.
- 1회차의 수동 갱신 시나리오에서 source와 Gold의 의도된 차이를 확인했다.

수업 사이에는 이 프로젝트를 savepoint와 함께 종료한다. 맥미니 전체 VM을 재시작하거나 다른 데모 작업을 중단하지 않는다. 장기 운영은 별도의 Iceberg 보존 정책과 공간 모니터링이 필요하다.

## 자료 확인

- 진행 시간 합계 120분.
- 학생 실습은 GitHub 자료 받기 → 예상값·원천 SQL 작성 → 강사 화면 관찰 → 개인 설계서 작성.
- SQL 파일과 shell/Python 도우미 구문 확인.
- 아키텍처 PNG 렌더링과 한글 표시 확인.
- 핵심 시연을 실제로 실행하고 source/Gold 기대값을 비교.

삭제 예고는 선택 단계이며 이번 핵심 리허설 결과 표에는 포함하지 않았다. 초기 설치를 수업의 120분 안에 넣지 않는다.
