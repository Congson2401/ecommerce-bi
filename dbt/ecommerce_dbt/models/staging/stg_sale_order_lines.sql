WITH source AS (
    SELECT * FROM {{ source('public', 'sale_order_line') }}
),

cleaned AS (
    SELECT
        id                  AS line_id,
        order_id,
        product_id,
        name                AS product_description,
        order_partner_id    AS customer_id,
        product_uom_qty     AS qty_ordered,
        price_unit,
        discount,
        price_subtotal      AS revenue_subtotal,
        price_total         AS revenue_total,
        state               AS line_status,
        write_date
    FROM source
    WHERE display_type IS NULL
)

SELECT * FROM cleaned