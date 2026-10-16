# 요약 화면·관찰 기록 리허설 — 2026-10-17

맥미니에서 새 키 `s01-review-20261017a-r1`로 payment 준비와 환불 c/u/d를 순서대로 실행했다. 각 단계에서 `wait --evidence` 뒤 `collect-check --stage … --summary --evidence`를 사용했다. 기존 10월 15일 기록을 재사용하지 않은 새 실행이다.

| 단계 | 원천 행 | op | before → after amount | offset | Bronze copies | wait 관찰 초 |
|---|---|---|---|---|---|---|
| insert | 1 | c | None → 20000 | 18 | 1 | 11.3 |
| update | 1 | u | 20000 → 30000 | 19 | 1 | 9.17 |
| delete | 0 | d | 30000 → None | 20 | 1 | 9.28 |

모두 토픽 `cdc.public.refunds`, partition 0이다. DELETE 뒤 Bronze에는 c/u/d 3건이 남고 각 Kafka 위치가 한 번씩 대응했다. Kafka null tombstone 1건은 Bronze에서 제외됐다. 세 단계 모두 전체 대조의 `passed=true`를 확인했다.

## 증거 읽기

- [INSERT 전체](evidence/collection-20261017/collection-insert.json) / [요약](evidence/collection-20261017/collection-insert-summary.json)
- [UPDATE 전체](evidence/collection-20261017/collection-update.json) / [요약](evidence/collection-20261017/collection-update-summary.json)
- [DELETE 전체](evidence/collection-20261017/collection-delete.json) / [요약](evidence/collection-20261017/collection-delete-summary.json)
- [DELETE wait 기록](evidence/collection-20261017/wait-delete.json)

`wait_observed_seconds`는 wait 명령 시작부터 조회로 처음 확인할 때까지다. DML 실행부터의 지연이나 정확한 Iceberg commit 지연이 아니다. wait는 2초 간격 대기를 포함한 최대 60회 조회이며 쿼리 실행 시간도 들어간다. kafka_ts·ingested_at과 ISO 관찰 시각은 이번 실행에서 UTC 기준이다. 실행은 한국 시간 10월 17일 00시대이며 JSON에는 10월 16일 UTC로 기록된다.

## 실행 환경과 종료

준비 중 VM 여유가 2GiB 아래로 내려가 preflight가 실행을 막았다. 원천·Kafka·Iceberg 볼륨을 보존하고 참조되지 않는 이전 빌드 이미지만 정리한 뒤, ready=true·약 2.83GiB를 확인하고 진행했다. 5GiB 이상 권장 기준에는 미달하므로 수업 전 재점검이 필요하다.

확인 뒤 `scripts/stop.sh`로 savepoint `file:/opt/flink/state/savepoints/savepoint-3d391e-99d32d28b68f`를 남겼다. cdc-study 프로젝트의 실행 컨테이너가 없는 것도 확인했다. 다른 프로젝트와 Lima VM은 중지하지 않았다.

검증 범위는 이 합성 키의 정상 변경 수집과 새 요약·wait 증거 연결이다. 2시간 부하, 장애 복구, 재발행 중복, 전체 파이프라인 exactly-once는 이번 실행으로 입증하지 않는다.
