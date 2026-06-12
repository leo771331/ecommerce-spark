# 인터뷰 대비 메모

## 1. 전체 처리 흐름

1. CSV를 명시적 schema로 읽는다.
2. event_time 문자열에서 UTC timestamp를 만든다.
3. UTC timestamp를 KST로 변환한다.
4. KST timestamp에서 event_date_kst를 만든다.
5. 세션화와 WAU 계산에 필요한 필수 컬럼이 null인 row를 제거한다.
6. user_id별 Window를 구성한다.
7. lag로 직전 이벤트 시간을 가져온다.
8. gap_seconds를 계산한다.
9. gap_seconds >= 300이면 새 세션으로 판단한다.
10. is_new_session 누적합으로 session_seq를 만든다.
11. generated_session_id를 만든다.
12. Parquet Snappy로 staging path에 저장한다.
13. partition 단위로 target에 commit한다.
14. Hive External Table로 조회한다.
15. WAU를 계산한다.

## 2. 자주 나올 수 있는 질문

### 왜 KST 변환 후 partition을 만들었나요?

요구사항이 KST 기준 daily partition이기 때문입니다. UTC 기준 날짜와 KST 기준 날짜는 다를 수 있으므로, 반드시 KST 변환 이후 날짜를 추출해야 합니다.

### 왜 gap >= 300인가요?

요구사항이 5분 이상인 경우 새 세션이라고 했기 때문에 정확히 5분, 즉 300초도 새 세션입니다.

### 왜 staging을 사용했나요?

target path에 바로 overwrite하면 배치 실패 시 기존 정상 partition이 손상될 수 있습니다. staging에 먼저 쓰고, 성공 후 partition 단위로 commit하면 실패 시 rollback할 수 있습니다.

### 왜 External Table인가요?

Spark가 생성한 Parquet 파일을 Hive가 외부 경로로 참조하기 위해서입니다. 데이터 저장 위치와 Hive metadata를 분리할 수 있습니다.

### 왜 deduplication을 하지 않았나요?

과제에서 중복 제거 기준을 정의하지 않았습니다. 이벤트 로그에서는 같은 시간에 유사 이벤트가 여러 개 존재할 수 있으므로 임의 deduplication은 데이터 의미를 바꿀 수 있습니다.

### 추가 기간이 들어오면 어떻게 처리하나요?

`--input` 또는 `INPUT_PATHS`에 추가 CSV 파일을 전달하면 됩니다. output path는 동일하게 유지하고, commit 로직이 처리 대상 `event_date_kst` partition을 교체합니다.
