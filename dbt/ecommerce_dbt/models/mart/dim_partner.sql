{{ config(materialized='table') }}

WITH ranked AS (
    SELECT
        *,
        NTILE(100) OVER (
            ORDER BY customer_rank DESC
        ) AS pct
    FROM {{ ref('stg_partners') }}
    WHERE active = true
      AND customer_rank > 0
),

non_customer AS (
    SELECT
        *,
        NULL::int AS pct
    FROM {{ ref('stg_partners') }}
    WHERE active = true
      AND customer_rank = 0
)

SELECT
    partner_id,
    partner_name,
    partner_type,
    city,
    state_name,
    country_name,
    is_company,
    customer_rank,
    supplier_rank,
    pct                             AS customer_percentile,
    CASE
        WHEN customer_rank = 0 THEN NULL
        WHEN pct <= 15         THEN 'VIP'
        WHEN pct <= 40         THEN 'Silver'
        ELSE                        'Regular'
    END                             AS customer_tier,
    pct <= 15 AND customer_rank > 0 AS is_vip
FROM ranked

UNION ALL

SELECT
    partner_id,
    partner_name,
    partner_type,
    city,
    state_name,
    country_name,
    is_company,
    customer_rank,
    supplier_rank,
    pct                             AS customer_percentile,
    NULL                            AS customer_tier,
    false                           AS is_vip
FROM non_customer