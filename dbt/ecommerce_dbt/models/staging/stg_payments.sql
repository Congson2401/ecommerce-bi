-- stg_payments.sql
-- Staging model for account_payment

WITH source AS (
    SELECT * FROM {{ source('public', 'account_payment') }}
),

cleaned AS (
    SELECT
        id                      AS payment_id,
        name                    AS payment_number,
        payment_type,
        partner_type,
        state                   AS payment_state,
        partner_id,
        company_id,
        currency_id,
        journal_id,
        amount                  AS payment_amount,
        date                    AS payment_date,
        create_date,
        write_date,

        -- Derived: payment direction
        CASE payment_type
            WHEN 'inbound'  THEN 'received'
            WHEN 'outbound' THEN 'sent'
            ELSE 'other'
        END AS payment_direction

    FROM source
    WHERE state = 'posted'
)

SELECT * FROM cleaned
