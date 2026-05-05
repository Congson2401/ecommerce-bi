WITH source AS (
    SELECT * FROM {{ source('public', 'product_supplierinfo') }}
),

cleaned AS (
    SELECT
        id                  AS supplierinfo_id,
        partner_id          AS supplier_id,
        product_id,
        price               AS price_agreed,
        delay               AS lead_time_committed,
        min_qty,
        write_date
    FROM source
)

SELECT * FROM cleaned