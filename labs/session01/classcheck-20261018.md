# 1회차 수업 전 재실행 — 2026-10-18

맥미니의 디스크 정리 후, 기존 savepoint에서 기동해 실제 1회차 실행 순서를 다시 검증했다. 과거 리허설을 재사용하지 않고 `s01-classcheck-20261018a`, `s01-classcheck-20261018b` 두 키를 새로 생성했다.

## 확인한 흐름

1. 기존 savepoint 복원과 preflight ready=true. 준비 단계의 Silver 갱신·dbt 2개 모델과 6개 테스트도 통과.
2. 첫 키로 payment 준비 → 환불 INSERT·UPDATE·DELETE. 수업 가이드의 source-rows·kafka·wait·collect-check 17개 명령 모두 종료 코드 0.
3. 각 단계 요약 화면을 60초 유지하며 체크포인트 상태 관찰. 실패 0.
4. savepoint 정상 종료 → `bootstrap.sh --no-build` 재기동. 이미지 ID가 유지되고 기존 Bronze 전체 해시·행 수·수집 위치가 동일.
5. 이전 키 DELETE 대조 재실행과 새 키의 전체 c/u/d를 다시 통과. 이후 savepoint로 정상 종료.

## 실제 단계 결과

모두 `cdc.public.refunds`, partition 0이다. wait 관찰 초는 대기 명령 시작 후 첫 조회까지이며 원천 commit부터의 정확한 지연이 아니다.

| 실행 | 단계 | 원천 행 | before → after amount | offset | copies | wait 관찰 초 |
|---|---|---|---|---|---|---|
| first | insert | 1 | null → 20000 | 22 | 1 | 7.08 |
| first | update | 1 | 20000 → 30000 | 23 | 1 | 9.22 |
| first | delete | 0 | 30000 → null | 24 | 1 | 7.02 |
| after-restart | insert | 1 | null → 20000 | 26 | 1 | 9.24 |
| after-restart | update | 1 | 20000 → 30000 | 27 | 1 | 9.2 |
| after-restart | delete | 0 | 30000 → null | 28 | 1 | 9.29 |

두 실행 모두 DELETE 뒤 원천 0행, Bronze c/u/d 3건, 각 위치 copies=1, Kafka null tombstone 1건을 확인했다. 수업 핵심 시연에서는 Silver·Gold refresh를 실행하지 않았다.

## 재기동·접속·리소스

- Bronze 전체 행 수: 33 → 첫 실습 뒤 38 → 재기동 뒤 38 → 두 번째 실습 뒤 43. 모든 시점에서 중복 수집 위치 0.
- 빌드·이미지 pull 없이 재기동하고 준비용 마트·테스트까지 마친 시간: 41.05초. 단일 실행의 관찰값이며 고정 소요 시간을 보장하지 않는다.
- savepoint 실제 복원은 Flink의 restored 정보와 저장한 경로를 대조했다. 두 번째 기동은 최근 실습에서 만든 savepoint를 사용했다.
- 강사 노트북의 localhost:28081 SSH 터널에서 UI HTML, JavaScript 파일, overview·jobs API HTTP 200을 확인했다. 실제 브라우저 렌더 검증이라고 표현하지 않는다.
- 최종 preflight ready=true, VM 여유 22.69GiB를 확인했고 별도 JSON에 보관한다. Bronze 메타데이터 자동 정리 설정 true·이전 버전 100 유지도 확인했다.
- 기존 DE7의 실행 중 Flink 컨테이너 두 개와 Lima VM은 유지했다. 이번 CDC 프로젝트만 savepoint 종료했다.

## 수업 당일 기동 명령 수정

기존 준비 명령은 이미지 빌드까지 수행해 의존성을 다시 다운로드하는 경우가 있었다. 첫 설치·이미지 수정은 `bootstrap.sh --build`, 수업 당일은 `bootstrap.sh --no-build`로 구분했다. 새 옵션은 `--no-build --pull never`로 로컬 이미지만 사용하며, 이미지가 없으면 중단한다. 설정·토픽·커넥터 등록과 savepoint 복원, 준비용 마트 갱신은 기존과 동일하다.

```bash
ssh mini
cd ~/bootcamp/cdc-lakehouse-study
./scripts/bootstrap.sh --no-build
./scripts/lab.sh exec -T runner python labs/session01/preflight.py
```

[실행 안내](runbook.md)의 단계대로 진행한다. 이미지 빌드는 수업 전에 끝낸다.

## 증거와 검증 범위

[증거 폴더](evidence/classcheck-20261018/)에는 두 실행의 전체 대조·요약·wait·명령별 시간·체크포인트 관찰, 재기동 전후 스냅샷, preflight, 이미지 ID 비교와 터널 접속 확인을 보관했다. 비밀번호가 포함될 수 있는 렌더링된 SQL·전체 설정·원본 빌드 로그는 배포하지 않는다.

정상 변경 수집, 단계별 60초 유지, 계획된 savepoint 정지·재기동과 재기동 후 수집을 검증했다. 2시간 연속 부하, 모든 네트워크 장애, TaskManager 강제 종료, 전체 데이터의 end-to-end exactly-once는 이번 검증 범위가 아니다. 시각은 증거 JSON에서 UTC로 기록되며 실제 실행일은 한국 시간 10월 18일이다.
