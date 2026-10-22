# 2회차 — PostgreSQL의 변경을 Kafka·Bronze·Parquet에서 따라가기

**120분 · 강사 맥미니 시연 + 학생 SQL·예측·진행 공유**

지난 시간에 같은 환불을 20,000원으로 입력하고 30,000원으로 정정한 뒤 삭제했다. 이번 시간에는 Bronze에 남은 c/u/d 세 이력과 실제 Parquet 파일 내용을 먼저 확인한다. 이어서 이 결과가 만들어지는 PostgreSQL 수집 조건, Debezium 설정, Kafka 메시지를 연결해 읽는다.

## 수업 자료

- [강의안 PPTX — 28장·발표자 노트](slides/02-session02.pptx) · [강의안 PDF](slides/02-session02.pdf)
- [강사용 진행안](instructor-guide.md) · [복사해서 실행하는 큐시트](cuesheet.md) · [큐시트 PDF](02-instructor-cuesheet.pdf)

- [학생 실습 가이드와 120분 흐름](student-guide.md)
- [SQL 학습지 — Spark SQL / PostgreSQL 구분](sql/student-worksheet.sql)
- [수업 후 복습 양식](submission-template.md)
- [독립 Python 파서](event_parser.py) · [고정 입력 예시](fixtures/events.jsonl)
- [공식 참고 문서](references.md)
- [1회차 복습 안내](../session01/student-guide.md)
- [전체 환경 요건과 기동 안내](../../README.md)

수업 중에는 강사 화면을 함께 보며 SQL과 기대값을 작성하고, 복습에서 어디까지 진행했는지와 막힌 지점을 공유한다. 학생별 서버를 기동하거나 새 복습 양식을 작성하는 시간은 아니다. 수업 후에는 개인 환경에서 공통 예제를 실행하고 SQL·관찰 결과·질문을 정리한다.

## 이번 시간에 확인할 것

1. 같은 환불이 원천에서는 삭제돼도 Bronze에는 c/u/d 이력으로 남는지 직접 조회한다.
2. Iceberg가 참조하는 Parquet 파일의 실제 내용과 Bronze 컬럼을 연결한다.
3. publication·replication slot·replica identity의 역할을 실제 설정에서 찾는다.
4. 초기 스냅샷 r과 새 변경 c/u/d, 삭제 이벤트와 tombstone을 구분한다.
5. 작은 Python 파서와 SQL로 같은 이벤트를 해석하고, 원천·Kafka·Bronze 중 어느 구간을 조사할지 판단한다.

## 120분 흐름

| 경과 시간 | 분 | 진행 |
|---|---:|---|
| 00:00–00:10 | 10 | 복습 진행 상황·막힌 지점 공유 |
| 00:10–00:30 | 20 | 이월 실습: Bronze c/u/d 세 이력·실제 Parquet 내용 |
| 00:30–00:50 | 20 | PostgreSQL 수집 조건·Debezium 설정 연결 |
| 00:50–00:55 | 5 | 휴식 |
| 00:55–01:15 | 20 | 스냅샷 r·독립 Python 파서·SQL 해석 |
| 01:15–01:35 | 20 | 새 CASE_ID c/u/d 예측·Kafka/Bronze/파서 대조 |
| 01:35–01:50 | 15 | 관찰 결과로 수집 구간 진단 |
| 01:50–02:00 | 10 | 정리·복습 안내 |

진행 공유는 이 스터디에서 구현 과정과 질문을 나누는 시간이다. 개인 도메인·테이블 설계 확정은 3회차에 진행한다. Silver·Gold 변환, dbt 모델 구현과 운영·복구 실험은 후속 회차에서 다룬다.

## 실행과 기록을 구분하기

명령은 저장소 루트에서 사용한다. 수업 중 실행 환경은 강사의 맥미니이며, 수업 후 복습은 각자 구성한 개인 환경이다. 강사 서버 접속 정보나 전용 설정을 복사할 필요는 없다. 현재 참고 환경은 Linux ARM 컨테이너를 실행할 수 있는 Docker 환경을 기준으로 한다.

파서 fixture는 메시지 해석을 확인하는 고정 예제다. fixture 통과, 컨테이너 RUNNING, slot active, 체크포인트 완료를 각각 전체 수집 성공으로 바꾸어 설명하지 않는다. 실제 실행에서 확인한 원천·Kafka·Bronze·파일 결과를 따로 대조한다.

복습 결과는 `submission.md`와 `student-worksheet.sql`로 정리한다. 캡처 이미지는 함께 첨부해 다른 사람이 열 수 있게 한다. 제출 방법과 기한은 강사 안내를 따른다.
