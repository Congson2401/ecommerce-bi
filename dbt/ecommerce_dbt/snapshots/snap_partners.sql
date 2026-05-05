{% snapshot snap_partners %}
{{
    config(
        target_schema='dw',
        unique_key='id',
        strategy='timestamp',
        updated_at='write_date'
    )
}}

SELECT
    p.id,
    p.name              AS partner_name,
    p.city,
    p.state_id,
    p.country_id,
    p.customer_rank,
    p.supplier_rank,
    p.is_company,
    p.active,
    p.write_date
FROM public.res_partner p
WHERE p.active = true

{% endsnapshot %}