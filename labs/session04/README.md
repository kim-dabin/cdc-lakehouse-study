# 4회차 — Spark SQL로 현재 상태를 만드는 Silver 설계

Bronze에는 같은 환불의 생성·정정·삭제가 각각 남습니다. 이 세 줄을 그대로 더하면 현재 환불 상태를 잘못 읽을 수 있습니다. 이번에는 **어떤 키로 묶고, 어떤 변경을 최신으로 고르며, 삭제 뒤 과거 변경을 어떻게 거절할지** 정한 뒤 Spark SQL의 `ROW_NUMBER`와 `MERGE`로 확인합니다.

공통 `orders`·`payments`·`refunds`의 의미는 같습니다. 결제는 100,000원이며 환불 20,000원을 생성하고 30,000원으로 정정한 뒤 철회합니다. 환불 행을 삭제해도 주문·결제는 유지됩니다. 수업의 비교 기준은 이 공통 예제이고, 개인 프로젝트의 주제는 각자 선택한 도메인입니다.

- [학생 가이드](student-guide.md): 변경 이력과 현재 상태, SQL의 판단 기준, 단계별 실행과 결과 읽기.
- [시연 따라 하기](follow-along/README.md): 시연 영상과 같은 순서로 복사할 명령, 기대 결과, 예측 질문을 모은 안내.
- [개인 Silver 설계 학습지](design-worksheet.md): 이번 주 도메인·원천 DDL 초안을 한 테이블의 Silver 계약과 SQL로 연결하기.
- [최신 후보 SQL](sql/01-latest.sql) · [MERGE SQL](sql/02-merge.sql) · [확인 SQL](sql/03-check.sql): 실행기가 채울 테이블 이름과 입력 범위가 들어 있는 템플릿.
- [실습 실행기](lab.sh): 기존 Docker 환경에서 전체 또는 단계별 실험 실행.
- [공식 참고 문서](references.md): Spark SQL·Iceberg 쓰기·commit·성능 판단의 근거.

## 이번 실험의 범위

**Bronze 규격의 고정 입력을 사용해 Silver 규칙을 검증합니다.** 이 실행기가 PostgreSQL에서 새 CDC를 발생시키거나 Kafka·Flink 수집을 재시연하는 것은 아닙니다. 별도 `lake.lesson04` 영역의 실습별 테이블을 사용하고 공통 Bronze·Silver·Gold를 갱신하지 않습니다. 실시간 수집 지연이나 운영 SLA를 측정하는 실험도 아닙니다.

`prepare`는 처음부터 c/u/d 세 이력을 모두 준비합니다. 설명용 LSN은 100/200/300, offset은 10/11/12입니다. 단계별 MERGE는 이미 있는 입력 중 LSN 상한을 100→200→300으로 바꿔 상태를 비교합니다. 이 상한은 수업용 재생 조건이며 운영 증분 처리의 watermark 구현이 아닙니다.

수업에서는 강사 화면을 보며 학생도 SQL과 결과를 먼저 예측하고, 각자의 원천 테이블에 적용할 규칙을 작성합니다. 집에서는 아래 실행기로 같은 실험을 직접 확인합니다.

## 집에서 한 번에 실행하기

아래는 **기존 Docker 환경이 실행 중인 경우**, 개인 컴퓨터의 저장소 루트에서 실행합니다. 처음 설치하거나 환경이 중지돼 있다면 [가이드의 환경 준비](student-guide.md#1-실행-환경-준비)를 먼저 봅니다.

```bash
RUN_ID="s04-$(date +%Y%m%d-%H%M%S)"
./labs/session04/lab.sh all --run-id "$RUN_ID"
```

`all`은 새 RUN_ID로 시작합니다. 이미 완료한 입력을 다시 적용하려면 같은 RUN_ID로 아래 명령을 사용합니다.

```bash
./labs/session04/lab.sh merge --stage all --run-id "$RUN_ID"
./labs/session04/lab.sh verify --run-id "$RUN_ID"
```

최종 기대는 Silver **현재 snapshot의 보존 행 1개**, `amount=30000`, `is_deleted=true`, `source_lsn=300`, `kafka_offset=12`입니다. **유효한 현재 환불은 0행**입니다. 과거 snapshot·옛 파일의 행까지 저장소 전체에서 한 개라는 뜻은 아닙니다. 보존된 30,000원은 철회 직전의 값입니다. 이 값이 현재 유효한 환불액이라는 뜻은 아닙니다.

새 RUN_ID의 `all` 완료 뒤 고정 Bronze 입력은 5행입니다. offset 12의 같은 삭제 전달이 2행 있고, 과거 UPDATE가 새 offset 13으로 추가됩니다. 이 과거 변경은 별도 입력 배치(LSN≤200)의 후보로 골라 기존 삭제 상태와 비교하므로 MERGE 버전 가드의 역할도 확인합니다. 이 전달 중복이 Silver의 업무 키 중복을 뜻하지는 않습니다.

결과는 `.runtime/session04/<RUN_ID>/report.json`에 누적됩니다. 출력의 `prepare.tables`에서 실제 테이블 이름을 확인하세요. RUN_ID의 하이픈은 테이블 이름에서 밑줄로 바뀝니다. [결과를 읽는 방법과 오류 확인](student-guide.md#5-결과와-로그-읽기)을 함께 봅니다.

이번 테이블은 `copy-on-write`를 명시합니다. 실제 실행에서는 **동일 입력을 다시 MERGE해도 업무 행은 같지만 새 overwrite snapshot과 데이터 파일이 생겼습니다.** 결과의 멱등성은 파일·snapshot이 변하지 않거나 실행 비용이 없다는 뜻이 아닙니다.

4회차의 결과는 개인 Silver 계약·SQL·검증 기준입니다. 5회차에는 이를 dbt Gold·테스트로 이어갑니다. 수업 중 새 제출 양식 작성을 요구하지 않으며 제출 방법·기한은 공지를 따릅니다. 최종 발표는 8회 스터디 이후 별도 일정입니다.
