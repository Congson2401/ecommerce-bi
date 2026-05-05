WITH source AS (
    SELECT * FROM {{ source('public', 'purchase_order') }}
),

cleaned AS (
    SELECT
        id                  AS po_id,
        name                AS po_name,
        partner_id          AS supplier_id,
        picking_type_id,
        date_order          AS order_date,
        date_planned        AS planned_date,
        effective_date      AS receipt_date,
        state               AS po_status,
        amount_untaxed,
        amount_tax,
        amount_total,
        CASE
            WHEN effective_date IS NOT NULL AND date_order IS NOT NULL
            THEN EXTRACT(DAY FROM effective_date - date_order)::INTEGER
            ELSE NULL
        END                 AS lead_time_actual,
        CASE
            WHEN date_planned IS NOT NULL AND date_order IS NOT NULL
            THEN EXTRACT(DAY FROM date_planned - date_order)::INTEGER
            ELSE NULL
        END                 AS lead_time_planned,
        write_date
    FROM source
    WHERE state != 'draft'
)

SELECT * FROM cleaned