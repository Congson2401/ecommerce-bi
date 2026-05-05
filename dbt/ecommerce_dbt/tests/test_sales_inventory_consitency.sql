WITH sales_qty AS (
    SELECT product_id, SUM(qty_sold) AS total_sold
    FROM {{ ref('fact_sales') }}
    WHERE order_status = 'done'
    GROUP BY product_id
),
inventory_qty AS (
    SELECT product_id, SUM(qty_out) AS total_out
    FROM {{ ref('fact_inventory') }}
    WHERE move_type = 'out'
    GROUP BY product_id
)
SELECT
    s.product_id,
    s.total_sold,
    i.total_out,
    ABS(s.total_sold - i.total_out) / NULLIF(s.total_sold, 0) AS diff_pct
FROM sales_qty s
JOIN inventory_qty i ON i.product_id = s.product_id
WHERE ABS(s.total_sold - i.total_out) / NULLIF(s.total_sold, 0) > 0.05