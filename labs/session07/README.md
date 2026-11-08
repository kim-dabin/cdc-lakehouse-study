# 7회차: 작은 커밋이 쌓이면 — Iceberg 유지보수와 개인 프로젝트 리뷰

스트리밍 수집과 반복 `MERGE`를 운영하다 보면 실제 데이터 행 수보다 스냅샷과 메타데이터 파일이 먼저 디스크를 채우는 일이 벌어집니다. 이번 회차의 전반부에서는 스터디 공통 환경에서 겪은 실제 디스크 포화 사고(데이터 27행, 메타데이터 3.6GB — 스터디 공통 환경 실측 2026-11-05)의 원인을 짚고, 격리 실험판(`lake.lesson07`)에서 작은 커밋 120번이 만든 데이터 파일·스냅샷·메타데이터를 `rewrite_data_files`·`expire_snapshots`·`remove_orphan_files`·`rewrite_manifests`와 테이블 속성으로 정리하는 순서 및 6회차 증분 `progress` 스냅샷 보존 기준을 확인합니다. 후반부에서는 9개 핵심 질문을 기준으로 개인 프로젝트를 상호 리뷰합니다.

공통 파이프라인의 테이블을 건드리지 않도록 `lake.lesson07` 아래 격리된 실험 테이블(`plain`, `tuned`, `inc`)과 데모 고정 실행 ID 5세트(`s07-demo-*`)를 사용합니다(코드: [`lab.py`](lab.py), [`demo.sh`](demo.sh)). 수업에서는 미리 실행해 둔 데모 테이블을 Lakehouse Explorer 예제로 열어 보며 결과를 예측·비교하고, 집에서는 개인 Docker 환경에서 `demo.sh`·Explorer·`lab.sh`로 같은 실험을 재현합니다.

- **실행 명령 요약** (저장소 루트에서 실행, 코드: [`demo.sh`](demo.sh), [`lab.sh`](lab.sh))
  ```bash
  # 데모 5세트(s07-demo-grow/compact/expire/clean/trap) 한 번에 준비
  ./labs/session07/demo.sh --fresh

  # 개인 실행 ID로 전 단계(prepare→grow→compact→expire→orphans→manifests→progress-trap→verify) 실행
  ./labs/session07/lab.sh all --run-id "s07-myname-01"
  ```
- **파일 목록**
  - [학생 가이드 (`student-guide.md`)](student-guide.md): 실제 메타데이터 폭증 사고 분석, Iceberg 파일 계층과 유지보수 도구 4종, `lesson07` 단계별 실측 비교, 6회차 증분 시작점(`progress`) 만료 함정, 운영 체크리스트, 집에서 따라 하기, 개인 프로젝트 리뷰 질문지 9개, 더 알아보기(파티션·정렬·쿼리 튜닝 참고 자료)
  - 코드: [`lab.py`](lab.py) · [`demo.sh`](demo.sh) · [`lab.sh`](lab.sh) · [`tools/explorer/README.md`](../../tools/explorer/README.md) · [`compose.yaml`](../../compose.yaml) · [`flink/ingest.sql.template`](../../flink/ingest.sql.template)
  - 슬라이드: [PDF](slides/07-session07-student.pdf) · [PPTX](slides/07-session07-student.pptx)

수업 중 별도 제출 절차는 없으며 과제 형식과 기한은 별도 공지를 따릅니다.
