# 1회차 CDC 수집 검증 리허설 — 2026-10-15

맥미니의 실제 PostgreSQL·Debezium/Kafka·Flink·Iceberg Bronze에서 새 키 `s01-de-rehearsal-20261015a-r1`에 입력·수정·삭제를 수행했다. 원천 조회, Kafka key/op/before/after, 같은 수집 위치의 Bronze 내용을 단계마다 대조했다. 수업 중 사용하는 collect-check 도우미의 세 단계가 통과했다.

| 단계 | 원천 해당 PK | Kafka non-null CDC 이벤트 | Bronze 기록 | 증거 |
|---|---|---|---|---|
| INSERT | 한 행, amount=20000 | c, offset 14 | 동일 위치 c | [입력](evidence/collection-20261015/collection-insert.json) |
| UPDATE | 한 행, amount=30000 | c/u, offset 14/15 | 동일 위치 c/u | [수정](evidence/collection-20261015/collection-update.json) |
| DELETE | 0행 | c/u/d, offset 14/15/16 | 동일 위치 c/u/d | [삭제](evidence/collection-20261015/collection-delete.json) |

모두 `cdc.public.refunds`, partition 0이다. UPDATE의 before.amount=20000, after.amount=30000을 확인했다. DELETE는 before.amount=30000, after=null이었다. Kafka에는 같은 key의 null tombstone이 offset 17에 있었고 Bronze에서는 이를 제외했다. 수집 성공 판정에서 tombstone을 non-null CDC 이벤트와 합산하지 않았다.

Kafka 조회 소비자는 group_id=None, 자동 offset commit 비활성으로 설정했으며 Flink consumer group을 사용하지 않는다. 초기 위치를 정하는 조회가 멈추는 문제를 발견해 beginning offset을 직접 조회한 후 seek하도록 수정하고 동일 시나리오를 다시 검증했다.

사전 preflight는 ready=true, VM 여유 약 2.12GiB였다. 이는 리허설 당시의 값이다. 수업 전 다시 점검하고 5GiB 이상 확보를 권장한다. 준비 bootstrap의 기존 Silver·Gold 갱신 이후, c/u/d 핵심 시연에서는 refresh를 실행하지 않았다. 확인 후 scripts/stop.sh로 이 프로젝트를 savepoint와 함께 종료했다.

이 기록은 한 합성 키의 수집 대조 결과다. 2시간 부하, 모든 테이블의 무누락, 재전달 멱등성·장애 복구·exactly-once는 이번 리허설의 검증 범위가 아니다. wait의 관찰 시간은 대기 명령 이후 첫 Bronze 조회까지이며 정확한 원천 commit 지연으로 해석하지 않는다.
