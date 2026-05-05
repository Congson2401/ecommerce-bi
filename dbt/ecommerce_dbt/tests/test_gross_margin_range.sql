SELECT id, gross_margin_pct
FROM {{ ref('fact_sales') }}
WHERE gross_margin_pct < -50 OR gross_margin_pct > 100