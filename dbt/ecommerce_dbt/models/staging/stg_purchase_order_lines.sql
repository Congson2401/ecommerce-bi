WITH source AS (
    SELECT * FROM {{ source('public', 'purchase_order_line') }}
),

cleaned AS (
    SELECT
        id                  AS pol_id,
        order_id            AS po_id,
        product_id,
        name                AS product_description,
        product_qty         AS qty_ordered,
        qty_received,
        price_unit,
        price_subtotal,
        price_total,
        write_date
    FROM source
    WHERE display_type IS NULL
)

SELECT * FROM cleaned