SELECT id, product_id, running_stock
FROM {{ ref('fact_inventory') }}
WHERE running_stock < -1000