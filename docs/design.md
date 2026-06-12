# 배치 설계 문서

## 1. 목적

이 배치는 이커머스 사용자 activity 로그를 Spark로 처리하여 sessionized event table을 만들고, 이를 Hive External Table로 제공하기 위해 작성했습니다.

## 2. 주요 설계

### 2.1 명시적 CSV Schema

대용량 CSV에서 schema inference를 사용하면 추가 scan 비용이 발생하고 실행 환경에 따라 타입 추론 결과가 달라질 수 있습니다.

따라서 CSV schema는 코드에 명시했습니다.

### 2.2 KST 기준 Partition

원본 event_time은 UTC입니다.

daily partition 요구사항은 KST 기준이므로, UTC timestamp를 KST로 변환한 뒤 `event_date_kst`를 생성했습니다.

### 2.3 Sessionization

동일 user_id 내 이벤트를 event_time_kst 기준으로 정렬하고, 직전 이벤트와의 간격이 300초 이상이면 새 세션으로 판단했습니다.

### 2.4 저장 방식

처리 결과는 Parquet + Snappy로 저장했습니다.

partition column은 `event_date_kst`입니다.

### 2.5 재처리 및 복구

target path에 직접 overwrite하지 않고 staging path에 먼저 저장합니다.

정상 저장 후 partition 단위로 target을 교체하고, 기존 target partition은 backup에 보관합니다.

정상 완료 후에는 `_job_status/run_id=.../_SUCCESS` marker를 남깁니다.
