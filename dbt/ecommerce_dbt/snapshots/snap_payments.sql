{% snapshot snap_payments %}

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
    payment_type,
    partner_type,
    state,
    partner_id,
    journal_id,
    amount,
    date,
    write_date
FROM {{ source('public', 'account_payment') }}
WHERE state = 'posted'

{% endsnapshot %}
