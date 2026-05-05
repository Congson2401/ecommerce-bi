WITH pickings AS (
    SELECT * FROM {{ ref('stg_stock_pickings') }}
    WHERE picking_type_id = 2
),

partners AS (
    SELECT * FROM {{ ref('stg_partners') }}
),

-- Thêm: aggregate qty từ stock moves để tính fill rate
moves AS (
    SELECT
        picking_id,
        SUM(qty_ordered)    AS qty_ordered,
        SUM(qty_delivered)  AS qty_delivered
    FROM {{ ref('stg_stock_moves') }}
    WHERE picking_type_id = 2
    GROUP BY picking_id
),

joined AS (
    SELECT
        p.picking_id,
        p.picking_name,
        p.partner_id,
        p.sale_id,
        p.location_src_id,
        p.location_dest_id,
        p.scheduled_date,
        p.date_done,
        p.picking_type_id,
        p.picking_status,
        p.delay_days,
        p.is_on_time,

        -- V3: threshold mới, bỏ 'unknown' → 'pending'
        CASE
            WHEN p.date_done IS NULL   THEN 'pending'        -- ← chưa giao
            WHEN p.delay_days <= 4     THEN 'on_time'
            WHEN p.delay_days <= 6     THEN 'slightly_late'  -- ← từ 4 lên 7
            WHEN p.delay_days <= 9    THEN 'late'
            ELSE                            'very_late'      -- ← giờ có data
        END                     AS delay_category,

        -- Thêm mới: fill rate từ stock moves
        m.qty_ordered,
        m.qty_delivered,
        CASE
            WHEN COALESCE(m.qty_ordered, 0) = 0 THEN NULL
            ELSE ROUND(
                m.qty_delivered::numeric / m.qty_ordered * 100, 2
            )
        END                     AS fill_rate_pct,

        -- Thêm mới: fill rate category
        CASE
            WHEN m.qty_ordered IS NULL          THEN NULL
            WHEN m.qty_delivered = m.qty_ordered THEN 'full'
            WHEN m.qty_delivered > 0             THEN 'partial'
            ELSE                                      'unfilled'
        END                     AS fill_category,

        pa.city,
        pa.country_name,
        p.write_date
    FROM pickings p
    LEFT JOIN partners pa ON pa.partner_id = p.partner_id
    LEFT JOIN moves m     ON m.picking_id  = p.picking_id
)

SELECT * FROM joined