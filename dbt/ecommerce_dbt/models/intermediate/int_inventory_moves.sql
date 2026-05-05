WITH moves AS (
    SELECT * FROM {{ ref('stg_stock_moves') }}
),

quant AS (
    SELECT * FROM {{ ref('stg_stock_quant') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

-- Bước 1: join data thô, KHÔNG reference bất kỳ cột nào chưa tồn tại
joined AS (
    SELECT
        m.move_id,
        m.product_id,
        m.picking_id,
        m.picking_type_id,
        m.sale_line_id,
        m.purchase_line_id,
        m.location_src_id,
        m.location_dest_id,
        m.move_date,
        m.move_type,
        m.move_status,
        CASE WHEN m.move_type = 'in'
            THEN m.qty_delivered ELSE 0
        END                                         AS qty_in,
        CASE WHEN m.move_type = 'out'
            THEN m.qty_delivered ELSE 0
        END                                         AS qty_out,
        m.qty_ordered,
        COALESCE(q.qty_reserved, 0)                 AS qty_reserved,
        COALESCE(q.qty_available, 0)                AS qty_available,
        COALESCE(p.standard_cost, 0)                AS standard_cost, -- ✅ giữ riêng để dùng sau
        GREATEST(m.write_date, q.write_date)        AS write_date
    FROM moves m
    LEFT JOIN quant q ON q.product_id = m.product_id
        AND q.location_id = m.location_dest_id
    LEFT JOIN products p ON p.product_id = m.product_id
),

-- Bước 2: tính running_stock — giờ mới có đủ data
with_running AS (
    SELECT
        *,
        SUM(
            CASE WHEN move_type = 'in'  THEN  qty_in
                 WHEN move_type = 'out' THEN -qty_out
                 ELSE 0
            END
        ) OVER (
            PARTITION BY product_id
            ORDER BY move_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                           AS running_stock,

        SUM(
            CASE WHEN move_type = 'in'  THEN  qty_in
                 WHEN move_type = 'out' THEN -qty_out
                 ELSE 0
            END
        ) OVER (
            PARTITION BY product_id
            ORDER BY move_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) * standard_cost                           AS running_inventory_value
        -- ✅ đơn giản: running_stock * standard_cost, không vòng vèo
    FROM joined
),

-- Bước 3: CTE cuối — lúc này running_stock đã tồn tại, mới dùng được
final AS (
    SELECT
        move_id,
        product_id,
        picking_id,
        picking_type_id,
        sale_line_id,
        purchase_line_id,
        location_src_id,
        location_dest_id,
        move_date,
        move_type,
        move_status,
        qty_in,
        qty_out,
        qty_ordered,
        GREATEST(running_stock, 0)                  AS running_stock,
        running_stock                               AS qty_on_hand,  -- ✅ giờ mới reference được
        qty_reserved,
        qty_available,
        GREATEST(running_inventory_value, 0)        AS running_inventory_value,
        write_date
    FROM with_running
)

SELECT * FROM final