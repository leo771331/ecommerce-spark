-- 6-a. WAU by user_id
-- user_id 기준 WAU는 KST 기준 event_date_kst가 속한 주차별 active user 수입니다.
-- 사용자는 여러 주차에 활동할 수 있으므로 각 주차에 한 번씩 카운트될 수 있습니다.
WITH user_week AS (
  SELECT DISTINCT
    date_sub(
      to_date(event_date_kst),
      pmod(dayofweek(to_date(event_date_kst)) + 5, 7)
    ) AS week_start_kst,
    user_id
  FROM ecommerce.user_activity_sessions
)
SELECT
  week_start_kst,
  COUNT(user_id) AS wau_by_user_id
FROM user_week
GROUP BY week_start_kst
ORDER BY week_start_kst;

-- 6-b. WAU by generated_session_id
-- 하나의 generated_session_id는 여러 event row에 반복될 수 있고,
-- 세션이 일자 또는 주차 경계를 넘을 수 있습니다.
-- 따라서 세션 기준 WAU는 event row의 event_date_kst가 아니라
-- generated_session_id별 session_start_time_kst 기준으로 주차를 계산합니다.
-- 즉, 하나의 세션은 세션이 시작된 주차에만 귀속됩니다.
WITH session_base AS (
  SELECT
    generated_session_id,
    MIN(session_start_time_kst) AS session_start_time_kst
  FROM ecommerce.user_activity_sessions
  GROUP BY generated_session_id
),
session_week AS (
  SELECT
    date_sub(
      to_date(session_start_time_kst),
      pmod(dayofweek(to_date(session_start_time_kst)) + 5, 7)
    ) AS week_start_kst,
    generated_session_id
  FROM session_base
)
SELECT
  week_start_kst,
  COUNT(generated_session_id) AS wau_by_generated_session_id
FROM session_week
GROUP BY week_start_kst
ORDER BY week_start_kst;
