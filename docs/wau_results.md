# WAU 결과

최종 WAU 결과는 Hive External Table `ecommerce.user_activity_sessions`를 기준으로 계산했습니다.

최종 batch run id:

~~~text
oct_nov_2019_v2
~~~

최종 output path:

~~~text
data/processed/user_activity_sessions
~~~

## 1. user_id 기준 WAU

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

## 2. generated_session_id 기준 WAU

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

## 3. 계산에 사용한 SQL

~~~sql
-- 6-a. WAU by user_id
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

-- 6-b. WAU by generated_session_id
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
