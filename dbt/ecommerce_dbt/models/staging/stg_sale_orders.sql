WITH source AS (
    SELECT * FROM {{ source('public', 'sale_order') }}
),

cleaned AS (
    SELECT
        id                  AS order_id,
        name                AS order_name,
        date_order          AS order_date,
        partner_id          AS customer_id,
        state               AS order_status,
        amount_untaxed      AS revenue_untaxed,
        amount_tax          AS tax_amount,
        amount_total        AS revenue_total,
        write_date
    FROM source
    WHERE state != 'draft'
)

SELECT * FROM cleaned