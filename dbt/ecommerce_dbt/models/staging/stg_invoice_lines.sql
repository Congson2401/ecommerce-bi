-- stg_invoice_lines.sql
-- Staging model for account_move_line (invoice detail lines)

WITH source AS (
    SELECT * FROM {{ source('public', 'account_move_line') }}
),

cleaned AS (
    SELECT
        id                      AS invoice_line_id,
        move_id                 AS invoice_id,
        product_id,
        account_id,
        journal_id,
        company_id,
        currency_id,
        partner_id,
        name                    AS line_description,
        display_type,
        parent_state,
        quantity,
        price_unit,
        discount,
        debit,
        credit,
        balance,
        amount_currency,
        price_subtotal,
        price_total,
        date                    AS line_date,
        create_date,
        write_date

    FROM source
    WHERE display_type = 'product'
      AND parent_state = 'posted'
)

SELECT * FROM cleaned
