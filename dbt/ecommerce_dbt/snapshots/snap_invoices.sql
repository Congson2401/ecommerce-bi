{% snapshot snap_invoices %}

{{
    config(
        target_schema='dw',
        unique_key='id',
        strategy='timestamp',
        updated_at='write_date'
    )
}}

SELECT
    id,
    name,
    move_type,
    state,
    payment_state,
    partner_id,
    journal_id,
    date,
    invoice_date,
    amount_untaxed,
    amount_tax,
    amount_total,
    amount_residual,
    write_date
FROM {{ source('public', 'account_move') }}
WHERE move_type IN ('out_invoice', 'out_refund')

{% endsnapshot %}
