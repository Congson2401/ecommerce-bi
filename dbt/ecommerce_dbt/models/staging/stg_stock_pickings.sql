WITH source AS (
    SELECT * FROM {{ source('public', 'stock_picking') }}
),

cleaned AS (
    SELECT
        id                  AS picking_id,
        name                AS picking_name,
        picking_type_id,
        partner_id,
        sale_id,
        location_id         AS location_src_id,
        location_dest_id,
        state               AS picking_status,
        scheduled_date,
        date_done,
        CASE
            WHEN date_done IS NOT NULL AND scheduled_date IS NOT NULL
            THEN EXTRACT(DAY FROM date_done - scheduled_date)::INTEGER
            ELSE NULL
        END                 AS delay_days,
        CASE
            WHEN date_done IS NOT NULL AND scheduled_date IS NOT NULL
            THEN date_done <= scheduled_date
            ELSE NULL
        END                 AS is_on_time,
        write_date
    FROM source
    WHERE state != 'draft'
)

SELECT * FROM cleaned