WITH template AS (
    SELECT * FROM {{ source('public', 'product_template') }}
),

variant AS (
    SELECT * FROM {{ source('public', 'product_product') }}
),

category AS (
    SELECT * FROM {{ source('public', 'product_category') }}
),

joined AS (
    SELECT
        pp.id               AS product_id,
        pt.id               AS template_id,
        pt.name->>'en_US'   AS product_name,
        pt.default_code     AS sku_code,
        pt.list_price,
        pt.list_price * 0.75 AS standard_cost,
        pt.type             AS product_type,
        pc.id               AS category_id,
        pc.name             AS category_name,
        pt.active
    FROM variant pp
    JOIN template pt ON pt.id = pp.product_tmpl_id
    LEFT JOIN category pc ON pc.id = pt.categ_id
    WHERE pp.active = true
)

SELECT * FROM joined