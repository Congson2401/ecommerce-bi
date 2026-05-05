{% snapshot snap_sale_order_lines %}
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
    order_partner_id,
    product_uom_qty,
    price_unit,
    discount,
    price_subtotal,
    price_total,
    state,
    write_date
FROM public.sale_order_line
WHERE display_type IS NULL
{% endsnapshot %}