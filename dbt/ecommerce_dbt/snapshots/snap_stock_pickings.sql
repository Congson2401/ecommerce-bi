{% snapshot snap_stock_pickings %}
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
    picking_type_id,
    partner_id,
    sale_id,
    location_id,
    location_dest_id,
    state,
    scheduled_date,
    date_done,
    write_date
FROM public.stock_picking
WHERE state != 'draft'
{% endsnapshot %}