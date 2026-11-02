# 6회차: 모델 계약·컬럼 리니지와 증분·소급 정정 설계하기

5회차에서 만든 Silver core와 Gold 마트는 매번 전체를 다시 계산해 테이블을 교체합니다. 이번 회차에서는 먼저 dbt 모델 계약(`contract: {enforced: true}`)과 OpenMetadata 컬럼 리니지로 열 변경의 파급을 확인한 뒤, 수정·삭제(철회)·과거 주문일 소급 정정·중간 실패 후 재시도가 일어나는 고정 입력 시나리오(`s1`~`s6`)에서 영향받은 키와 날짜만 골라 갱신하면서 전체 재계산과 일치시키는 증분 설계와 운영 기준을 다룹니다.

공통 파이프라인의 테이블을 건드리지 않도록 `lake.lesson06` 아래 격리된 고정 입력 테이블에서 여섯 시나리오(`s1`~`s6`)와 실패·재시도 함정(`--naive-dates`)을 비교합니다. 수업에서는 미리 실행해 둔 데모 테이블을 Lakehouse Explorer와 OpenMetadata 화면으로 열어 보며 결과와 SQL을 예측하고, 집에서는 개인 Docker 환경에서 `demo.sh`·Explorer·`lab.sh` 및 선택 스택인 OpenMetadata로 같은 내용을 재현합니다.

- [학생 가이드](student-guide.md): dbt 모델 계약과 Spark 제약 한계, 컬럼 리니지, 증분 도입 판단과 다섯 완료 조건, `create_changelog_view`와 COW carry-over 확인, 키 전파와 과거 날짜 이동, Gold MERGE와 `plan`·`progress`·스냅샷 만료 운영 기준, dbt `incremental` 비교, `s1`~`s6` 및 `naive` 재시도 비교, 집에서 `demo.sh`·Explorer·`lab.sh`·OpenMetadata 따라 하기, 내 프로젝트로 옮기기, 더 알아보기(Time Travel 교차 검증·재시도 전 새 변경 유입 설계)
- 슬라이드: [PDF](slides/06-session06-student.pdf) · [PPTX](slides/06-session06-student.pptx)
- 코드: [`demo.sh`](demo.sh) · [`lab.sh`](lab.sh) · [`lab.py`](lab.py) · [`sql/`](sql/) · [`tools/explorer/README.md`](../../tools/explorer/README.md) · [`docs/governance.md`](../../docs/governance.md) · [`contract-failure-demo.sh`](../../governance/contract-failure-demo.sh)

과제 형식과 기한은 별도 공지를 따릅니다.
