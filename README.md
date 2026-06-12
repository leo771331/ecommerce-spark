# Ecommerce Spark Sessionization

Kaggle ecommerce behavior dataset(2019-Oct.csv, 2019-Nov.csv)을 Spark로 처리하고, KST 기준 sessionized event table을 Hive External Table로 제공하는 과제입니다.

---

## 실행 환경

| 항목 | 값 |
|---|---|
| Language | Scala 2.12.18 |
| Build Tool | sbt |
| Spark | 3.5.8 |
| Java | 11 |
| Storage Format | Parquet / Snappy |
| Table Type | Hive External Table |
| OS | WSL2 Ubuntu |

Spark가 Scala 기반이고, Window 함수나 `lag` 같은 sessionization 로직을 Java보다 간결하게 표현할 수 있어 Scala를 선택했습니다.

---

## 처리 흐름

```
2019-Oct.csv, 2019-Nov.csv
  → 명시적 schema로 CSV read
  → event_time UTC → KST 변환, event_date_kst 생성
  → 필수 컬럼(event_time_kst, user_id) null row 제거
  → user_id별 5분 기준 sessionization → generated_session_id 생성
  → Parquet + Snappy, event_date_kst partition 저장
  → staging → target commit (실패 시 backup rollback)
  → Hive External Table 생성
  → WAU 계산
```

**sessionization 기준:** 동일 `user_id` 내 직전 이벤트와 gap이 300초 이상이면 새 세션.

```
gap_seconds = 299 → 같은 세션
gap_seconds = 300 → 새 세션  ← 경계값 포함
```

생성된 session ID 예시:
```
512742880_20191002000000_000001
512742880_20191002000959_000002
```

---

## 프로젝트 구조

```
ecommerce-spark/
├── build.sbt
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
    └── verification_checklist.md
```

---

## 설계 결정 메모

**KST 기준 partition**
원본 `event_time`은 UTC라 daily partition을 UTC 날짜 기준으로 자르면 KST 사용자 기준으로 하루가 엇갈린다. 변환 후 `event_date_kst`를 추출해 partitionBy했고, 결과적으로 `2019-10-01` ~ `2019-12-01` 범위의 partition이 생성됐다.

```
2019-10-31 18:00:00 UTC → 2019-11-01 03:00:00 KST → event_date_kst=2019-11-01
```

**staging commit 방식**
target path에 바로 overwrite하지 않고, `_staging/run_id=<run_id>`에 먼저 쓴 뒤 partition 단위로 교체한다. 기존 partition은 `_backup`에 보관하고, 실패 시 rollback. 전체 성공 후 `_job_status/run_id=.../_SUCCESS` marker를 남긴다.

**Hive External Table**
Spark가 Parquet 파일을 쓰고 Hive는 그 경로를 참조만 한다. 데이터 삭제와 테이블 metadata 삭제 책임이 분리되고, 나중에 LOCATION만 바꾸면 HDFS/S3로 이전할 수 있다.

**null 처리 / deduplication**
`event_time_kst`, `user_id`가 null이면 세션화 기준을 만들 수 없으므로 제외했다. `brand`, `price` 등 부가 컬럼은 null이어도 보존. 과제에서 중복 제거 기준을 정의하지 않아 임의 deduplication은 하지 않았다.

---

## WAU 계산 쿼리

주 시작일은 월요일 기준. week_start 계산식:

```sql
date_sub(
  to_date(event_date_kst),
  pmod(dayofweek(to_date(event_date_kst)) + 5, 7)
)
```

쿼리 전문: [`sql/wau_queries.sql`](sql/wau_queries.sql)

### user_id 기준 WAU

| week_start_kst | wau_by_user_id |
|---|---:|
| 2019-09-30 | 374245 |
| 2019-10-07 | 521107 |
| 2019-10-14 | 535673 |
| 2019-10-21 | 550069 |
| 2019-10-28 | 757635 |
| 2019-11-04 | 876972 |
| 2019-11-11 | 862640 |
| 2019-11-18 | 862094 |
| 2019-11-25 | 760503 |
| 2019-12-02 | 20197 |

### generated_session_id 기준 WAU

| week_start_kst | wau_by_generated_session_id |
|---|---:|
| 2019-09-30 | 943978 |
| 2019-10-07 | 1300869 |
| 2019-10-14 | 1354278 |
| 2019-10-21 | 1406010 |
| 2019-10-28 | 1938867 |
| 2019-11-04 | 2204989 |
| 2019-11-11 | 2157846 |
| 2019-11-18 | 2176918 |
| 2019-11-25 | 1924702 |
| 2019-12-02 | 51165 |

---

## 실행 방법

```bash
# 빌드
./scripts/build.sh

# 샘플 데이터 생성 및 실행
./scripts/create_sample_data.sh
DEBUG_OUTPUT=true RUN_ID=sample_debug ./scripts/run_local.sh

# Kaggle 데이터 다운로드 (data/raw/2019-Oct.csv, 2019-Nov.csv)
./scripts/download_data.sh

# 전체 Oct/Nov 처리 (WSL 환경, nohup 권장)
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

# 성공 확인
cat data/processed/user_activity_sessions/_job_status/run_id=oct_nov_2019_v2/_SUCCESS

# Hive External Table 생성
spark-sql -e "DROP TABLE IF EXISTS ecommerce.user_activity_sessions;"
TABLE_LOCATION="file://$(realpath data/processed/user_activity_sessions)"
spark-sql --hivevar table_location="$TABLE_LOCATION" -f sql/create_external_table.sql

# WAU 계산
spark-sql -f sql/wau_queries.sql
```

---

## 검증 결과

| 항목 | 결과 |
|---|---|
| sbt compile / package | ✅ |
| UTC → KST 변환 | ✅ |
| gap 299초 → 같은 세션 | ✅ |
| gap 300초 → 새 세션 | ✅ |
| staging / backup / commit | ✅ |
| 전체 Oct/Nov Spark 처리 | ✅ |
| Hive External Table 생성 및 partition 등록 | ✅ |
| user_id / session 기준 WAU 계산 | ✅ |

---

## Git 관리 기준

대용량 원천 데이터와 Spark 산출물은 제외하고, 재현 가능한 코드/SQL/스크립트/문서만 관리한다.

```
# .gitignore
data/raw/*
data/sample/*
data/processed/*
target/
logs/
metastore_db/
spark-warehouse/
```

---

## AI 활용

ChatGPT를 설계 검토, 코드 리뷰, 오류 분석, 문서, 코드 작성 초안 작성에 보조적으로 활용했습니다. 실제 환경 구성, 코드 수정, 실행 및 결과 검증은 직접 수행했습니다. 제출 코드는 라인 단위로 설명할 수 있도록 확인하면서 작성했습니다.
