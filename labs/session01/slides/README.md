# 1회차 강의용 PPT

[데이터 엔지니어링 중심 강의 자료](session01-data-engineering-reviewed-v2.pptx)

총 39장입니다. 1–34장은 휴식 5분을 포함한 120분 수업, 35–39장은 채용 근거·강사 사전 준비·시연 명령·개인 DDL·Bronze 조회 SQL 부록입니다. 슬라이드 하단은 수업 시작 후 경과 시간입니다. 같은 시간 구간의 보조 슬라이드는 해당 구간 안에서 함께 사용하며 시간을 중복 배정하지 않습니다.

도입 18분에는 CDC를 쓰는 이유, 배치·증분 조회와의 차이, 채용공고에서 확인하는 설계·운영 역량과 개인 프로젝트 예시를 설명합니다.

핵심은 원천 INSERT·UPDATE·DELETE를 Kafka와 Bronze에서 대조하고 개인 파이프라인을 설계하는 것입니다. 학생은 시연 전 기대값을 작성하고 시연 중 세 값만 기록합니다. 개인 설계는 안내 5분 뒤 체크리스트를 20분 고정하며 핵심 테이블 하나의 명세·DDL·기대 이벤트까지만 필수로 작성합니다. 강사는 맥미니에서 실행하며 학생 서버 설치·SSH 접속은 필요하지 않습니다. 집계 SQL과 마트 모델링은 이후 회차로 연결합니다.

아키텍처 세 관점은 유지하고 이전 마트 시연 수치·1회차 수동 갱신 표기를 수정했습니다. PPT에서는 옆 요약을 제거하고 그림을 크게 배치했습니다. 전체 프로젝트 그림이므로 Spark·dbt·Gold도 표시하지만, 이번 수업의 확인 대상은 PostgreSQL부터 Iceberg Bronze까지입니다. 텍스트와 표·SQL은 PPT에서 편집할 수 있습니다. 그림의 편집 원본은 [draw.io 자료](../../../docs/diagrams/README.md)를 사용합니다.

발표자 노트에 질문, 예상 답, 학생 활동과 시연 순서를 적었습니다. PowerPoint 또는 Keynote에서 노트를 함께 열어 사용하세요. 사용 글꼴은 Apple SD Gothic Neo와 D2Coding입니다.

- [CDC 필요성과 프로젝트별 구현·검증 범위](../why-cdc-and-projects.md)
- [학생 실습 가이드](../student-guide.md)
- [강사 진행안](../instructor-guide.md)
- [맥미니 실행 순서](../runbook.md)
- [학생 SQL](../sql/student-worksheet.sql)
- [제출 설계서](../submission-template.md)
- [최신 수집 검증 리허설](../collection-rehearsal-20261017.md)

이전 `session01-cdc-lakehouse-v3.pptx`와 `session01-data-engineering.pptx`, `session01-data-engineering-v2.pptx`는 수정 전 초안으로 보관합니다. 이번 수업에는 위 데이터 엔지니어링 버전을 사용합니다. 8회까지가 스터디이며 프로젝트 발표는 별도 일정입니다.
