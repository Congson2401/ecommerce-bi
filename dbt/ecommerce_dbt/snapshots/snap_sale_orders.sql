{% snapshot snap_sale_orders %}
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
    partner_id,
    date_order,
    state,
    amount_untaxed,
    amount_tax,
    amount_total,
    write_date
FROM public.sale_order
WHERE state != 'draft'
{% endsnapshot %}