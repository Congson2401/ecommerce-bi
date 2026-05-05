SELECT id, qty_sold
FROM {{ ref('fact_sales') }}
WHERE qty_sold <= 0