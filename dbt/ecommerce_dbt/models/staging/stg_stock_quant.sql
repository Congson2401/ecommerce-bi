WITH source AS (
    SELECT * FROM {{ source('public', 'stock_quant') }}
),

cleaned AS (
    SELECT
        id                          AS quant_id,
        product_id,
        location_id,
        quantity                    AS qty_on_hand,
        reserved_quantity           AS qty_reserved,
        quantity - reserved_quantity AS qty_available,
        in_date,
        write_date
    FROM source
    WHERE quantity > 0
)

SELECT * FROM cleaned