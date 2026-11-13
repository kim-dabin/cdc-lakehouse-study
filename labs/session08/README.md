# 8회차: 설계 이유 총정리와 장애 대응

마지막 회차에서는 새로운 도구나 기능을 더하지 않고, 1~7회차 동안 만든 CDC 레이크하우스 파이프라인의 **설계 이유와 실무 고려 사항을 한 장의 지도로 묶고**, 스터디 공통 환경에서 실제로 겪은 **디스크 고갈·연쇄 중단 장애의 진단과 복구 과정**을 점검합니다(설계: [`labs/README.md`](../README.md)).

우리가 만든 구조는 "유일한 정답"이 아니라, 주어진 요구사항과 운영 복잡도 사이에서 트레이드오프를 따져 고른 한 가지 조합입니다. 전반부에서는 수집(Debezium·Kafka)부터 Bronze(Flink)·Silver 현재 상태(Spark MERGE)·Silver core 및 Gold(dbt)·증분 갱신·Iceberg 유지보수·거버넌스·Airflow 스케줄까지 각 층에서 무엇을 골랐고 무엇을 포기했는지 정리합니다. 후반부에서는 2026년 11월 5일 스터디 공통 환경에서 발생한 실제 사고와 11월 13일 근본 복구 실측을 따라가며, 장애가 났을 때 어느 순서로 원인을 좁히고 어디서부터 재처리를 시작해야 하는지 다룹니다(스터디 공통 환경 실측 2026-11-05·2026-11-13).

- **집에서 실행해 볼 명령 요약** (저장소 루트에서 실행, 코드: [`scripts/recovery.sh`](../../scripts/recovery.sh), [`scripts/stop.sh`](../../scripts/stop.sh), 문서: [`docs/operations.md`](../../docs/operations.md), [`docs/follow-along.md`](../../docs/follow-along.md))
  ```bash
  # 1) TaskManager 중지 중 원천 변경 → 복구 후 Bronze·Gold 정합성 검증
  ./scripts/recovery.sh

  # 2) Airflow(선택) 기동 및 DAG 등록 오류 확인 (Paused 유지 권장)
  ./scripts/lab.sh --profile orchestration up -d airflow
  ./scripts/lab.sh exec -T airflow airflow dags list-import-errors

  # 3) 실습 종료 시 Flink savepoint를 남기고 안전하게 중지
  ./scripts/stop.sh
  ```
- **파일 목록**
  - [학생 가이드 (`student-guide.md`)](student-guide.md): 1~7회차 층별 설계 지도, 핵심 질문 6개와 설계 결정 트레이드오프, Airflow 책임 경계, 스터디 공통 환경 장애 진단·복구·검증 실측, 층별 재처리 시작점, 집에서 해 볼 복구 실험과 환경 점검, 내 프로젝트 장애 양식과 최종 운영 점검표, 발표 안내
  - 관련 문서·코드: [`docs/operations.md`](../../docs/operations.md) · [`docs/follow-along.md`](../../docs/follow-along.md) · [`scripts/recovery.sh`](../../scripts/recovery.sh) · [`scripts/recovery.py`](../../scripts/recovery.py) · [`scripts/stop.sh`](../../scripts/stop.sh) · [`scripts/flink_state.py`](../../scripts/flink_state.py) · [`airflow/dags/lakehouse_refresh.py`](../../airflow/dags/lakehouse_refresh.py)
  - Lakehouse Explorer 8회차 예제 8개([`tools/explorer`](../../tools/explorer/README.md)): 따로 준비할 테이블 없이 공통 파이프라인에서 층별 행 수 대조, Gold 일별 정산, Bronze가 보관한 Kafka 위치, Bronze snapshot·metadata.json 수, 최근 커밋과 Flink checkpoint, 테이블 속성을 조회합니다. 복구 전후나 `stop.sh` 전후에 같은 예제를 눌러 비교해 보세요.
  - 슬라이드: [PDF](slides/08-session08-student.pdf) · [PPTX](slides/08-session08-student.pptx)

수업 중 별도 제출 절차는 없으며, 프로젝트 발표는 별도 날짜에 진행하고 발표·과제 형식과 기한은 별도 공지를 따릅니다.
