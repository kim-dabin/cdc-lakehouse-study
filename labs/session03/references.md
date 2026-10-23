# 3회차 공식 참고 문서

2026-10-23 확인. 기술을 선택한 이유와 결과를 판단하는 경계를 더 읽어볼 자료입니다. 수업의 상세 설정·명령 목록을 늘리기 위한 자료는 아닙니다.

| 설명할 내용 | 공식 문서 | 읽을 부분 |
|---|---|---|
| PostgreSQL의 변경을 논리적으로 읽기 | [PostgreSQL 17 Logical Decoding](https://www.postgresql.org/docs/17/logicaldecoding-explanation.html) | 변경 전달과 replication slot의 역할 |
| 스냅샷과 변경 이벤트 | [Debezium 3.2 PostgreSQL Connector](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html) | 스냅샷·변경·삭제 이벤트 |
| Kafka의 소비 위치와 다시 읽기 | [Kafka 3.9 Design](https://kafka.apache.org/39/design/design/) | Consumer Position, Message Delivery Semantics |
| 처리 상태를 복구하는 checkpoint | [Flink 1.20 Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/checkpointing/) | 재생 가능한 입력과 영속 상태의 조건 |
| Flink에서 Iceberg에 쓰기 | [Iceberg 1.10 Flink Writes](https://iceberg.apache.org/docs/1.10.0/flink-writes/) | checkpoint와 commit, commit 관측 지표 |
| 테이블 상태를 조회로 확인하기 | [Iceberg 1.10 Spark Queries](https://iceberg.apache.org/docs/1.10.0/spark-queries/) | snapshots·history 메타데이터 조회 |
| 테이블 커밋과 읽기 일관성 | [Iceberg 1.10 Reliability](https://iceberg.apache.org/docs/1.10.0/reliability/) | 테이블 metadata와 커밋의 역할 |

이 예제의 Bronze는 raw CDC 원문을 append하고 null tombstone을 제외합니다. 현재 상태를 고르는 책임은 후속 Silver에 있습니다. 개별 checkpoint 완료, 테이블 snapshot, 같은 키의 실제 SELECT는 서로 다른 관찰이며 번호를 일대일로 맞추지 않습니다.

학습지의 두 지연은 프로젝트 목표입니다. helper wait의 관찰 시간이나 예제 설정을 고정 SLA로 표현하지 않습니다. 이번 수업은 복구 조건을 이해하는 단계이며 장애 주입·간격 변경 실험은 후속 범위입니다.
