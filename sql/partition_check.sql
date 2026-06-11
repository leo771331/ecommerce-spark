SHOW PARTITIONS ecommerce.user_activity_sessions;

SELECT
    event_date_kst,
    COUNT(*) AS row_count
FROM ecommerce.user_activity_sessions
GROUP BY event_date_kst
ORDER BY event_date_kst;

SELECT
    user_id,
    event_time_kst,
    event_date_kst,
    generated_session_id
FROM ecommerce.user_activity_sessions
ORDER BY user_id, event_time_kst
LIMIT 20;
