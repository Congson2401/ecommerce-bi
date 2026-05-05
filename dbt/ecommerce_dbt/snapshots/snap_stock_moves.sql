{% snapshot snap_stock_moves %}
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
    product_id,
    location_id,
    location_dest_id,
    picking_id,
    picking_type_id,
    sale_line_id,
    purchase_line_id,
    product_uom_qty,
    state,
    date,
    write_date
FROM public.stock_move
WHERE state = 'done'
{% endsnapshot %}