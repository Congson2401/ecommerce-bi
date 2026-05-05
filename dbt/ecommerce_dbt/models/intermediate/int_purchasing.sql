WITH orders AS (
    SELECT * FROM {{ ref('stg_purchase_orders') }}
),

lines AS (
    SELECT * FROM {{ ref('stg_purchase_order_lines') }}
),

suppliers AS (
    SELECT * FROM {{ ref('stg_supplier_info') }}
),

inbound_pickings AS (
    SELECT * FROM {{ ref('stg_stock_pickings') }}
    WHERE picking_type_id = 1
),

-- FIX #3: deduplicate pickings trước khi join
-- Mỗi cặp (supplier, khoảng thời gian) chỉ giữ 1 picking gần nhất
-- tránh fan-out nhiều-nhiều khi join với PO lines
deduped_pickings AS (
    SELECT DISTINCT ON (ip.partner_id, o.po_id)
        o.po_id,
        ip.picking_id,
        ip.picking_name,
        ip.picking_status,
        ip.scheduled_date,
        ip.date_done,
        ip.delay_days,
        ip.is_on_time
    FROM inbound_pickings ip
    JOIN orders o
        ON  ip.partner_id = o.supplier_id
        AND ip.date_done::date BETWEEN
            o.order_date::date AND
            COALESCE(o.receipt_date::date, o.planned_date::date + 30)
    ORDER BY
        ip.partner_id,
        o.po_id,
        ip.date_done ASC  -- lấy picking sớm nhất cho mỗi PO
),

joined AS (
    SELECT
        l.pol_id,
        o.po_id,
        o.po_name,
        l.product_id,
        o.supplier_id,
        o.picking_type_id,
        o.order_date,
        o.po_status,
        o.planned_date,
        o.receipt_date,
        o.lead_time_actual,
        o.lead_time_planned,

        CASE
            WHEN o.receipt_date IS NOT NULL
             AND o.planned_date IS NOT NULL
            THEN o.receipt_date <= o.planned_date
            ELSE NULL
        END                     AS is_on_time,

        CASE
            WHEN o.lead_time_actual  IS NOT NULL
             AND o.lead_time_planned IS NOT NULL
            THEN o.lead_time_actual - o.lead_time_planned
            ELSE NULL
        END                     AS lead_time_variance,

        CASE
            WHEN o.receipt_date IS NULL                               THEN 'pending'
            WHEN o.receipt_date <= o.planned_date                     THEN 'on_time'
            WHEN o.lead_time_actual - o.lead_time_planned <= 3        THEN 'slightly_late'
            WHEN o.lead_time_actual - o.lead_time_planned <= 7        THEN 'late'
            ELSE                                                           'very_late'
        END                     AS lead_time_category,

        -- ✅ join với deduped_pickings thay vì inbound_pickings trực tiếp
        dp.picking_id,
        dp.picking_name,
        dp.picking_status,
        dp.scheduled_date       AS picking_scheduled_date,
        dp.date_done            AS picking_date_done,
        dp.delay_days           AS picking_delay_days,
        dp.is_on_time           AS picking_is_on_time,

        l.qty_ordered,
        l.qty_received,
        l.price_unit,
        l.price_subtotal        AS purchase_amount,
        CASE
            WHEN l.qty_ordered > 0
            THEN ROUND(l.qty_received::numeric / l.qty_ordered * 100, 2)
            ELSE 0
        END                     AS receipt_rate_pct,

        s.price_agreed,
        CASE
            WHEN s.price_agreed IS NOT NULL AND s.price_agreed > 0
            THEN ROUND(l.price_unit / s.price_agreed * 100, 2)
            ELSE NULL
        END                     AS price_vs_agreed_pct,
        CASE
            WHEN s.price_agreed IS NOT NULL
            THEN l.price_unit <= s.price_agreed * 1.05
            ELSE NULL
        END                     AS is_price_compliant,

        s.lead_time_committed,
        s.min_qty,

        GREATEST(o.write_date, l.write_date) AS write_date

    FROM lines l
    JOIN orders o ON o.po_id = l.po_id
    LEFT JOIN suppliers s
        ON  s.supplier_id = o.supplier_id
        AND s.product_id  = l.product_id
    -- ✅ join đơn giản theo po_id, đã được deduplicate ở trên
    LEFT JOIN deduped_pickings dp ON dp.po_id = o.po_id
)

SELECT * FROM joined