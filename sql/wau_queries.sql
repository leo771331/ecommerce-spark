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