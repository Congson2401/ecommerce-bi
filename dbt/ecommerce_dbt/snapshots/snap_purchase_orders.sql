{% snapshot snap_purchase_orders %}
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
    picking_type_id,
    date_order,
    date_planned,
    effective_date,
    state,
    amount_untaxed,
    amount_tax,
    amount_total,
    write_date
FROM public.purchase_order
WHERE state != 'draft'
{% endsnapshot %}