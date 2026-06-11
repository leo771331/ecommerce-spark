CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.user_activity_sessions (
    event_time STRING,
    event_type STRING,
    product_id BIGINT,
    category_id BIGINT,
    category_code STRING,
    brand STRING,
    price DOUBLE,
    user_id BIGINT,
    user_session STRING,
    event_time_utc TIMESTAMP,
    event_time_kst TIMESTAMP,
    prev_event_time_kst TIMESTAMP,
    gap_seconds BIGINT,
    is_new_session INT,
    session_seq BIGINT,
    session_start_time_kst TIMESTAMP,
    generated_session_id STRING
)
PARTITIONED BY (
    event_date_kst STRING
)
STORED AS PARQUET
LOCATION "${hivevar:table_location}"
TBLPROPERTIES (
    'parquet.compress' = 'SNAPPY',
    'description' = 'Sessionized ecommerce user activity logs partition by KST event date'
);

MSCK REPAIR TABLE ecommerce.user_activity_sessions;
