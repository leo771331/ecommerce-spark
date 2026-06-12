# Ecommerce Spark Sessionization

## 1. 개요

이 프로젝트는 이커머스 사용자 activity 로그를 Spark Application으로 처리하고, 처리 결과를 Hive External Table로 제공한 뒤 WAU를 계산하는 과제입니다.

처리 대상 데이터는 Kaggle의 ecommerce behavior dataset 중 아래 두 파일입니다.

- `2019-Oct.csv`
- `2019-Nov.csv`

최종 처리 흐름은 다음과 같습니다.

~~~text
2019-Oct.csv, 2019-Nov.csv
  → 명시적 schema로 CSV read
  → event_time UTC timestamp 파싱
  → UTC → KST 변환
  → KST 기준 event_date_kst 생성
  → 세션화와 WAU 계산에 필요한 필수 컬럼 정제
  → user_id별 5분 기준 sessionization
  → generated_session_id 생성
  → Parquet + Snappy 저장
  → event_date_kst 기준 partition 저장
  → staging / backup / job_status 기반 commit
  → Hive External Table 생성
  → user_id 기준 WAU 계산
  → generated_session_id 기준 WAU 계산
~~~

## 2. 실행 환경

| 항목 | 값 |
|---|---|
| Language | Scala |
| Build Tool | sbt |
| Spark | 3.5.8 |
| Scala | 2.12.18 |
| Java | 11 |
| Storage Format | Parquet |
| Compression | Snappy |
| Table Type | Hive External Table |
| Local Environment | WSL2 Ubuntu |

## 3. Scala 선택 이유

과제에서 Spark Application 구현 언어는 Scala 또는 Java로 제한되어 있습니다.

이 프로젝트에서는 Scala를 선택했습니다.

선택 이유는 다음과 같습니다.

- Spark 자체가 Scala 기반으로 만들어졌고, Spark SQL/DataFrame API를 Scala에서 자연스럽게 사용할 수 있습니다.
- 세션화 구현에 필요한 Window 함수, `lag`, 누적합, 컬럼 변환 로직을 Java보다 간결하게 표현할 수 있습니다.
- 과제의 핵심인 CSV 처리, timestamp 변환, sessionization, Parquet 저장, Hive 연동을 Spark API 중심으로 구현하기에 Scala가 더 적합하다고 판단했습니다.

## 4. 프로젝트 구조

~~~text
ecommerce-spark/
├── README.md
├── build.sbt
├── project/
│   └── build.properties
├── src/main/scala/com/ecommerce/spark/
│   └── EcommerceSessionApp.scala
├── scripts/
│   ├── build.sh
│   ├── create_sample_data.sh
│   ├── download_data.sh
│   ├── run_local.sh
│   └── run_full_local.sh
├── sql/
│   ├── create_external_table.sql
│   ├── partition_check.sql
│   └── wau_queries.sql
└── docs/
    ├── ai_usage.md
    ├── design.md
    ├── interview_guide.md
    ├── verification_checklist.md
    └── wau_results.md
~~~

## 5. Spark Application 처리 흐름

메인 애플리케이션은 다음 파일에 있습니다.

~~~text
src/main/scala/com/ecommerce/spark/EcommerceSessionApp.scala
~~~

처리 순서는 다음과 같습니다.

1. CSV 파일을 명시적 schema로 읽습니다.
2. 원본 `event_time` 문자열에서 UTC timestamp를 생성합니다.
3. UTC timestamp를 KST 기준 timestamp로 변환합니다.
4. KST timestamp에서 `event_date_kst`를 생성합니다.
5. 세션화와 WAU 계산에 필요한 필수 컬럼이 없는 row를 제외합니다.
6. 동일 `user_id` 내에서 `event_time_kst` 기준으로 이벤트를 정렬합니다.
7. `lag`를 사용해 직전 이벤트 시간을 구합니다.
8. 현재 이벤트와 직전 이벤트의 간격을 초 단위로 계산합니다.
9. 간격이 300초 이상이면 새 세션으로 판단합니다.
10. 새 세션 flag를 누적합하여 `session_seq`를 생성합니다.
11. `user_id`, `session_start_time_kst`, `session_seq`를 조합하여 `generated_session_id`를 생성합니다.
12. 결과를 Parquet + Snappy로 저장합니다.
13. `event_date_kst` 기준으로 partition 저장합니다.
14. staging / backup / job_status 방식으로 commit합니다.

## 6. KST 기준 Daily Partition

원본 `event_time`은 UTC 기준입니다.

따라서 daily partition은 원본 UTC 날짜가 아니라 KST 변환 이후의 날짜를 기준으로 생성했습니다.

예시:

~~~text
2019-10-31 18:00:00 UTC
→ 2019-11-01 03:00:00 KST
→ event_date_kst = 2019-11-01
~~~

따라서 `2019-Nov.csv`만 처리하더라도 KST 기준으로는 `2019-12-01` partition이 생성될 수 있습니다.

최종 전체 데이터 처리 결과에서는 다음 범위의 partition이 확인되었습니다.

~~~text
event_date_kst=2019-10-01
...
event_date_kst=2019-12-01
~~~

## 7. 세션화 기준

요구사항은 다음과 같습니다.

~~~text
동일 user_id 내에서 event_time 간격이 5분 이상인 경우 세션 종료로 간주하고 새로운 세션 ID를 생성
~~~

따라서 새 세션 판단 조건은 다음과 같습니다.

~~~text
gap_seconds >= 300
~~~

세션화 로직 요약:

~~~text
user_id별 Window 구성
→ event_time_kst 기준 정렬
→ lag(event_time_kst)로 직전 이벤트 시간 계산
→ gap_seconds 계산
→ 첫 이벤트 또는 gap_seconds >= 300이면 is_new_session = 1
→ is_new_session 누적합으로 session_seq 생성
→ generated_session_id 생성
~~~

샘플 검증 결과:

~~~text
gap_seconds = 299 → 같은 세션
gap_seconds = 300 → 새 세션
gap_seconds = 360 → 새 세션
~~~

생성된 session ID 예시:

~~~text
512742880_20191002000000_000001
512742880_20191002000959_000002
554748717_20191001090000_000001
554748717_20191001090900_000002
~~~

## 8. 필수 컬럼 정제

전체 로그 데이터에는 파싱 실패 또는 필수값 누락 row가 있을 수 있습니다.

세션화와 WAU 계산에 필요한 기준 컬럼이 없는 row는 계산 기준을 만들 수 없으므로 제외했습니다.

제외 기준:

~~~text
event_time_utc IS NULL
event_time_kst IS NULL
event_date_kst IS NULL
user_id IS NULL
~~~

단, 다음 부가 컬럼은 null이어도 보존했습니다.

~~~text
brand
category_code
price
user_session
~~~

이 컬럼들은 세션화와 WAU 계산의 필수 기준이 아니며, 실제 ecommerce 로그에서 null일 수 있기 때문입니다.

또한 과제에서 중복 제거 기준을 별도로 정의하지 않았기 때문에 임의의 deduplication은 수행하지 않았습니다.

## 9. 저장 포맷과 파티션

저장 포맷:

~~~text
Parquet
~~~

압축:

~~~text
Snappy
~~~

Partition column:

~~~text
event_date_kst
~~~

출력 경로:

~~~text
data/processed/user_activity_sessions
~~~

최종 출력 구조 예시:

~~~text
data/processed/user_activity_sessions/
├── event_date_kst=2019-10-01/
├── event_date_kst=2019-10-02/
├── ...
└── event_date_kst=2019-12-01/
~~~

## 10. 재처리 및 장애 복구 전략

단순히 target path에 바로 overwrite하지 않고, run 단위 staging 경로에 먼저 저장한 뒤 partition 단위로 commit합니다.

commit 흐름:

~~~text
1. _staging/run_id=<run_id> 경로에 먼저 저장
2. 저장이 성공하면 처리 대상 event_date_kst partition 목록 확인
3. 기존 target partition이 있으면 _backup/run_id=<run_id>로 이동
4. staging partition을 target partition 위치로 이동
5. partition 교체 중 실패하면 backup에서 rollback
6. 모든 partition commit 성공 후 _job_status/run_id=<run_id>/_SUCCESS 생성
7. _staging/run_id=<run_id> 삭제
~~~

성공 marker 예시:

~~~text
data/processed/user_activity_sessions/_job_status/run_id=oct_nov_2019_v2/_SUCCESS
~~~

이 구조를 통해 같은 기간을 재처리하더라도 성공적으로 staging write가 끝난 뒤에만 target partition을 교체할 수 있습니다.

## 11. Hive External Table

Hive External Table DDL은 다음 파일에 있습니다.

~~~text
sql/create_external_table.sql
~~~

테이블명:

~~~text
ecommerce.user_activity_sessions
~~~

External Table로 설계한 이유:

- Spark Application이 물리적인 Parquet output path를 생성합니다.
- Hive는 해당 외부 경로를 참조만 합니다.
- 데이터 삭제와 테이블 metadata 삭제의 책임을 분리할 수 있습니다.
- 추후 HDFS/S3 등 외부 storage 경로로 바꾸더라도 DDL의 LOCATION만 바꾸면 됩니다.

Partition column:

~~~text
event_date_kst
~~~

`event_date_kst`는 Spark에서 `partitionBy("event_date_kst")`로 저장한 컬럼이므로 Hive DDL에서는 일반 컬럼 목록이 아니라 `PARTITIONED BY`에 선언했습니다.

## 12. WAU 계산 기준

WAU는 KST 기준 날짜 컬럼인 `event_date_kst`를 기준으로 계산했습니다.

주 시작일은 월요일로 계산했습니다.

week_start 계산식:

~~~sql
date_sub(
  to_date(event_date_kst),
  pmod(dayofweek(to_date(event_date_kst)) + 5, 7)
)
~~~

계산 대상:

- `user_id` 기준 WAU: 주차별 `COUNT(DISTINCT user_id)`
- `generated_session_id` 기준 WAU: 주차별 `COUNT(DISTINCT generated_session_id)`

쿼리 파일:

~~~text
sql/wau_queries.sql
~~~

## 13. WAU 계산 쿼리

### 13-a. user_id 기준 WAU

~~~sql
WITH base AS (
  SELECT
    date_sub(
      to_date(event_date_kst),
      pmod(dayofweek(to_date(event_date_kst)) + 5, 7)
    ) AS week_start_kst,
    user_id
  FROM ecommerce.user_activity_sessions
  WHERE event_date_kst IS NOT NULL
    AND user_id IS NOT NULL
)
SELECT
  week_start_kst,
  COUNT(DISTINCT user_id) AS wau_by_user_id
FROM base
GROUP BY week_start_kst
ORDER BY week_start_kst;
~~~

### 13-b. generated_session_id 기준 WAU

~~~sql
WITH base AS (
  SELECT
    date_sub(
      to_date(event_date_kst),
      pmod(dayofweek(to_date(event_date_kst)) + 5, 7)
    ) AS week_start_kst,
    generated_session_id
  FROM ecommerce.user_activity_sessions
  WHERE event_date_kst IS NOT NULL
    AND generated_session_id IS NOT NULL
)
SELECT
  week_start_kst,
  COUNT(DISTINCT generated_session_id) AS wau_by_generated_session_id
FROM base
GROUP BY week_start_kst
ORDER BY week_start_kst;
~~~

## 14. WAU 결과

### 14-a. user_id 기준 WAU

| week_start_kst | wau_by_user_id |
|---|---:|
| 2019-09-30 | 818388 |
| 2019-10-07 | 1057958 |
| 2019-10-14 | 1090898 |
| 2019-10-21 | 1093146 |
| 2019-10-28 | 1054722 |
| 2019-11-04 | 1321141 |
| 2019-11-11 | 1543309 |
| 2019-11-18 | 1376755 |
| 2019-11-25 | 1176254 |

### 14-b. generated_session_id 기준 WAU

| week_start_kst | wau_by_generated_session_id |
|---|---:|
| 2019-09-30 | 1570536 |
| 2019-10-07 | 2154180 |
| 2019-10-14 | 2257214 |
| 2019-10-21 | 2153837 |
| 2019-10-28 | 2115233 |
| 2019-11-04 | 2751842 |
| 2019-11-11 | 4754423 |
| 2019-11-18 | 2876494 |
| 2019-11-25 | 2376156 |

## 15. 실행 방법

### 15.1 빌드

~~~bash
./scripts/build.sh
~~~

### 15.2 샘플 데이터 생성

~~~bash
./scripts/create_sample_data.sh
~~~

### 15.3 샘플 실행

~~~bash
DEBUG_OUTPUT=true \
RUN_ID=sample_debug \
./scripts/run_local.sh
~~~

### 15.4 Kaggle 데이터 다운로드

~~~bash
./scripts/download_data.sh
~~~

다운로드 후 로컬에 아래 파일이 있어야 합니다.

~~~text
data/raw/2019-Oct.csv
data/raw/2019-Nov.csv
~~~

원천 CSV는 대용량 파일이므로 Git에 포함하지 않습니다.

### 15.5 전체 Oct/Nov 데이터 처리

로컬 WSL 환경에서는 전체 실행 시 로그가 많아 터미널이 멈출 수 있으므로 `nohup`으로 실행했습니다.

~~~bash
nohup spark-submit \
  --class com.ecommerce.spark.EcommerceSessionApp \
  --master 'local[2]' \
  --driver-memory 4g \
  --conf spark.sql.shuffle.partitions=800 \
  --conf spark.sql.files.maxPartitionBytes=67108864 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.local.dir="$(pwd)/data/processed/_spark_local_tmp" \
  target/scala-2.12/ecommerce-spark_2.12-0.1.0-SNAPSHOT.jar \
  --input data/raw/2019-Oct.csv,data/raw/2019-Nov.csv \
  --output data/processed/user_activity_sessions \
  --run-id oct_nov_2019_v2 \
  > logs/oct_nov_2019_v2.log 2>&1 &
~~~

성공 여부는 다음 marker로 확인했습니다.

~~~bash
cat data/processed/user_activity_sessions/_job_status/run_id=oct_nov_2019_v2/_SUCCESS
~~~

### 15.6 Hive External Table 생성

~~~bash
spark-sql -e "DROP TABLE IF EXISTS ecommerce.user_activity_sessions;"

TABLE_LOCATION="file://$(realpath data/processed/user_activity_sessions)"

spark-sql \
  --hivevar table_location="$TABLE_LOCATION" \
  -f sql/create_external_table.sql
~~~

### 15.7 Partition 확인

~~~bash
spark-sql -f sql/partition_check.sql
~~~

### 15.8 WAU 계산

~~~bash
spark-sql -f sql/wau_queries.sql
~~~

## 16. 검증 결과 요약

| 검증 항목 | 결과 |
|---|---|
| sbt compile | 성공 |
| sbt package | 성공 |
| 샘플 CSV read | 성공 |
| UTC → KST 변환 | 성공 |
| KST 기준 event_date_kst 생성 | 성공 |
| 299초 gap 같은 세션 처리 | 성공 |
| 300초 gap 새 세션 처리 | 성공 |
| Parquet Snappy 저장 | 성공 |
| staging / backup / job_status commit | 성공 |
| Kaggle Oct/Nov 데이터 다운로드 | 성공 |
| 전체 Oct/Nov Spark 처리 | 성공 |
| Hive External Table 생성 | 성공 |
| partition 등록 확인 | 성공 |
| user_id 기준 WAU 계산 | 성공 |
| generated_session_id 기준 WAU 계산 | 성공 |

## 17. AI 도구 활용 및 직접 검증 범위

본 과제 진행 중 ChatGPT를 설계 보조 및 구현 지원 도구로 활용했습니다.

AI 활용 범위:

~~~text
- 요구사항 분해
- Spark 처리 흐름 설계 검토
- Scala/Spark 코드 구조 리뷰
- 오류 원인 분석
- Git commit 단위 계획 수립
- README 및 문서 초안 작성 보조
~~~

직접 수행 및 검증한 범위:

~~~text
- WSL 개발 환경 구성
- GitHub repository 생성 및 push
- Scala Spark Application 직접 작성/수정
- sbt compile/package 실행
- 샘플 데이터 생성 및 Spark 실행 검증
- 전체 Oct/Nov 데이터 다운로드
- 전체 Spark batch 실행
- Hive External Table 생성
- partition check 실행
- WAU 쿼리 실행 및 결과 확인
~~~

프롬프트 전략:

~~~text
- 한 번에 전체 코드를 생성하지 않고 작은 commit 단위로 진행
- 각 단계마다 목적, 수정 파일, 검증 방법, commit 메시지를 분리
- 이해하지 못하는 코드는 제출하지 않는 방향으로 진행
- 오류 발생 시 로그를 기반으로 원인을 확인하고 수정
- 최종 결과는 직접 실행한 로그와 쿼리 결과 기준으로 문서화
~~~

## 18. Git 관리 기준

다음 파일과 디렉터리는 Git에 포함하지 않습니다.

~~~text
data/raw/*
data/sample/*
data/processed/*
target/
logs/
metastore_db/
spark-warehouse/
~~~

대용량 원천 데이터와 Spark 산출물은 Git에 올리지 않고, 재현 가능한 코드, SQL, 실행 스크립트, 문서만 관리합니다.
