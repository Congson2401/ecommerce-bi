{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,
    sku_code,
    category_id,
    category_name       AS category,
    product_type,
    list_price,
    standard_cost,
    CASE
        WHEN list_price > 0
        THEN ROUND((list_price - standard_cost) / list_price * 100, 2)
        ELSE 0
    END                 AS margin_pct
FROM {{ ref('stg_products') }}
WHERE active = true