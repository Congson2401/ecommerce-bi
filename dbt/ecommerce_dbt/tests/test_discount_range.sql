SELECT id, discount
FROM {{ ref('fact_sales') }}
WHERE discount < 0 OR discount > 100