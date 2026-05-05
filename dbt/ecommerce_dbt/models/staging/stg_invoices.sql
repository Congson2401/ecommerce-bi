-- stg_invoices.sql
-- Staging model for account_move (invoices & credit notes)

WITH source AS (
    SELECT * FROM {{ source('public', 'account_move') }}
),

cleaned AS (
    SELECT
        id                      AS invoice_id,
        name                    AS invoice_number,
        move_type,
        state                   AS invoice_state,
        payment_state,
        partner_id,
        company_id,
        currency_id,
        journal_id,
        date                    AS invoice_date,
        invoice_date            AS invoice_date_original,
        amount_untaxed,
        amount_tax,
        amount_total,
        amount_residual,
        create_date,
        write_date,

        -- Derived: human-readable invoice type
        CASE move_type
            WHEN 'out_invoice' THEN 'customer_invoice'
            WHEN 'out_refund'  THEN 'customer_refund'
            WHEN 'in_invoice'  THEN 'vendor_bill'
            WHEN 'in_refund'   THEN 'vendor_refund'
            ELSE 'other'
        END AS invoice_type_label,

        -- Flag: is this a refund/credit note?
        CASE WHEN move_type IN ('out_refund', 'in_refund') THEN TRUE ELSE FALSE END AS is_refund

    FROM source
    WHERE state != 'cancel'
      AND move_type IN ('out_invoice', 'out_refund')
)

SELECT * FROM cleaned
