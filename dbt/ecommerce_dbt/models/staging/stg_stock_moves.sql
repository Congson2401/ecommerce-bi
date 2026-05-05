WITH source AS (
    SELECT * FROM {{ source('public', 'stock_move') }}
),

locations AS (
    SELECT * FROM {{ source('public', 'stock_location') }}
),

cleaned AS (
    SELECT
        sm.id                   AS move_id,
        sm.product_id,
        sm.location_id          AS location_src_id,
        sm.location_dest_id,
        sm.picking_id,
        sm.picking_type_id,
        sm.sale_line_id,
        sm.purchase_line_id,
        sm.quantity             AS qty_delivered,
        sm.product_uom_qty      AS qty_ordered,
        sm.state                AS move_status,
        sm.date                 AS move_date,
        CASE
            WHEN src.usage = 'supplier'  THEN 'in'
            WHEN dest.usage = 'customer' THEN 'out'
            WHEN src.usage = 'internal'
             AND dest.usage = 'internal' THEN 'internal'
            WHEN dest.usage = 'inventory' THEN 'adjustment'
            ELSE 'other'
        END                     AS move_type,
        sm.write_date
    FROM source sm
    LEFT JOIN locations src  ON src.id = sm.location_id
    LEFT JOIN locations dest ON dest.id = sm.location_dest_id
    WHERE sm.state = 'done'
)

SELECT * FROM cleaned