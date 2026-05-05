{{ config(materialized='table') }}

WITH all_dates AS (
    SELECT date_order::date AS dt FROM {{ source('public', 'sale_order') }}
    WHERE date_order IS NOT NULL
    UNION
    SELECT date_order::date FROM {{ source('public', 'purchase_order') }}
    WHERE date_order IS NOT NULL
    UNION
    SELECT scheduled_date::date FROM {{ source('public', 'stock_picking') }}
    WHERE scheduled_date IS NOT NULL
    UNION
    SELECT date_done::date FROM {{ source('public', 'stock_picking') }}
    WHERE date_done IS NOT NULL
    UNION
    SELECT date::date FROM {{ source('public', 'stock_move') }}
    WHERE date IS NOT NULL
),

date_spine AS (
    SELECT generate_series(
        MIN(dt),
        MAX(dt),
        interval '1 day'
    )::date AS date
    FROM all_dates
)

SELECT
    TO_CHAR(date, 'YYYYMMDD')::integer  AS date_id,
    date,
    EXTRACT(DAY FROM date)::integer      AS day,
    TO_CHAR(date, 'Day')                 AS day_name,
    EXTRACT(DOW FROM date)::integer      AS day_of_week,
    EXTRACT(WEEK FROM date)::integer     AS week,
    EXTRACT(MONTH FROM date)::integer    AS month,
    TO_CHAR(date, 'Month')               AS month_name,
    EXTRACT(QUARTER FROM date)::integer  AS quarter,
    EXTRACT(YEAR FROM date)::integer     AS year,
    CASE
        WHEN EXTRACT(DOW FROM date) IN (0, 6)
        THEN true ELSE false
    END                                  AS is_weekend,
    CASE
        WHEN TO_CHAR(date, 'MM-DD') IN (
            '01-01', '04-30', '05-01',
            '09-02', '11-20', '12-25'
        )
        THEN true ELSE false
    END                                  AS is_holiday
FROM date_spine