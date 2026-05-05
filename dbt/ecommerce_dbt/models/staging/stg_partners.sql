WITH partner AS (
    SELECT * FROM {{ source('public', 'res_partner') }}
),

country_state AS (
    SELECT * FROM {{ source('public', 'res_country_state') }}
),

country AS (
    SELECT * FROM {{ source('public', 'res_country') }}
),

joined AS (
    SELECT
        p.id                AS partner_id,
        p.name              AS partner_name,
        CASE
            WHEN p.customer_rank > 0 AND p.supplier_rank > 0 THEN 'both'
            WHEN p.customer_rank > 0 THEN 'customer'
            WHEN p.supplier_rank > 0 THEN 'supplier'
            ELSE 'other'
        END                 AS partner_type,
        p.city,
        cs.name             AS state_name,
        c.name              AS country_name,
        p.customer_rank,
        p.supplier_rank,
        p.is_company,
        p.active
    FROM partner p
    LEFT JOIN country_state cs ON cs.id = p.state_id
    LEFT JOIN country c ON c.id = p.country_id
    WHERE p.active = true
)

SELECT * FROM joined