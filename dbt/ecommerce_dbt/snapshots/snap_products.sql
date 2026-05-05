{% snapshot snap_products %}
{{
    config(
        target_schema='dw',
        unique_key='id',
        strategy='timestamp',
        updated_at='write_date'
    )
}}

SELECT
    pt.id,
    pt.name->>'en_US'       AS product_name,
    pt.default_code         AS sku_code,
    pt.list_price,
    pt.list_price * 0.75    AS standard_cost,
    pt.type                 AS product_type,
    pc.id                   AS category_id,
    pc.name                 AS category_name,
    pt.active,
    pt.write_date
FROM public.product_template pt
LEFT JOIN public.product_category pc ON pc.id = pt.categ_id
WHERE pt.active = true

{% endsnapshot %}