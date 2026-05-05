{% snapshot snap_purchase_order_lines %}
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
    order_id,
    product_id,
    name,
    product_qty,
    qty_received,
    price_unit,
    price_subtotal,
    price_total,
    write_date
FROM public.purchase_order_line
{% endsnapshot %}