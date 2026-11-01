# 5회차: dbt로 Silver core와 Gold 지표, 테스트 설계하기

4회차에서 만든 Silver에는 엔티티 한 행의 현재 상태와 삭제 표시가 남아 있습니다. 이번에는 그 위에 여러 마트가 함께 쓰는 Silver core(공통 fact·dim)를 만들고, Silver core로 정산 담당자가 볼 Gold 지표를 정의해 dbt 모델과 데이터 테스트로 코드에 남기는 방법을 다룹니다. 공통 예제의 결제는 100,000원이고 환불 30,000원이 철회되면 순결제액은 100,000원이어야 합니다.

이번 회차는 설계와 코드 읽기가 중심이라 수업 중에 새 실습을 실행하지 않고 저장소의 [`dbt/`](../../dbt/) 코드와 공통 환경에서 미리 확인한 출력을 함께 읽습니다.

- [학생 가이드](student-guide.md): fact·dim을 어느 층에 둘지, Silver core와 Gold 설계 순서, dbt의 역할, 지표 정의·집계 단위·셀 행의 조건·fan-out, 데이터 테스트, table 모델과 갱신 주기, 설명·계보·소유·계약, 집에서 조회만 해 보기
- 슬라이드: [PDF](slides/05-session05-student.pdf) · [PPTX](slides/05-session05-student.pptx)
- 코드: [`models/silver/`](../../dbt/models/silver/) · [`mart_order_settlement.sql`](../../dbt/models/mart_order_settlement.sql) · [`mart_daily_settlement.sql`](../../dbt/models/mart_daily_settlement.sql) · [`sources.yml`](../../dbt/models/sources.yml) · [`settlement_balance.sql`](../../dbt/tests/settlement_balance.sql)

과제 형식과 기한은 별도 공지를 따릅니다.
