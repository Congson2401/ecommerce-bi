WITH expected AS (
    SELECT DISTINCT
        EXTRACT(YEAR FROM date)::int  AS yr,
        EXTRACT(MONTH FROM date)::int AS mo
    FROM {{ ref('dim_date') }}
    WHERE date BETWEEN '2023-01-01' AND '2025-06-30'
),
actual AS (
    SELECT
        EXTRACT(YEAR FROM d.date)::int  AS yr,
        EXTRACT(MONTH FROM d.date)::int AS mo,
        COUNT(*) AS cnt
    FROM {{ ref('fact_sales') }} fs
    JOIN {{ ref('dim_date') }} d ON d.date_id = fs.date_id
    WHERE fs.order_status = 'done'
    GROUP BY 1,2
)
SELECT e.yr, e.mo
FROM expected e
LEFT JOIN actual a ON a.yr = e.yr AND a.mo = e.mo
WHERE a.cnt IS NULL OR a.cnt < 10