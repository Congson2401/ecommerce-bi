{% snapshot snap_stock_quant %}
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
    quantity,
    reserved_quantity,
    quantity - reserved_quantity AS qty_available,
    in_date,
    write_date
FROM public.stock_quant
WHERE quantity > 0
{% endsnapshot %}