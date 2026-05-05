WITH orders AS (
    SELECT * FROM {{ ref('stg_sale_orders') }}
),

lines AS (
    SELECT * FROM {{ ref('stg_sale_order_lines') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

joined AS (
    SELECT
        l.line_id,
        o.order_id,
        o.order_name,
        l.product_id,
        o.customer_id,
        o.order_date,
        o.order_status,
        l.qty_ordered                               AS qty_sold,
        l.price_unit,
        l.discount,
        l.revenue_subtotal                          AS revenue,

        ROUND(l.qty_ordered * LEAST(
            COALESCE(p.standard_cost, l.price_unit * 0.75),
            l.price_unit * (1 - COALESCE(l.discount, 0)::numeric / 100) * 0.95
        ), 0)                                           AS cost,

        ROUND(l.revenue_subtotal - l.qty_ordered * LEAST(
            COALESCE(p.standard_cost, l.price_unit * 0.75),
            l.price_unit * (1 - COALESCE(l.discount, 0)::numeric / 100) * 0.95
        ), 0)                                           AS gross_margin,

        CASE
            WHEN l.revenue_subtotal > 0
            THEN ROUND(
                (l.revenue_subtotal - l.qty_ordered * LEAST(
                    COALESCE(p.standard_cost, l.price_unit * 0.75),
                    l.price_unit * (1 - COALESCE(l.discount, 0)::numeric / 100) * 0.95
                )) / l.revenue_subtotal::numeric * 100, 2
            )
            ELSE 0
        END                                         AS gross_margin_pct,

        GREATEST(o.write_date, l.write_date)        AS write_date
    FROM lines l
    JOIN orders o ON o.order_id = l.order_id
    LEFT JOIN products p ON p.product_id = l.product_id
)

SELECT * FROM joined