{{ config(materialized='incremental', unique_key='id') }}
{%- set _dim_partner  = ref('dim_partner') -%}
{%- set _dim_location = ref('dim_location') -%}

WITH delivery AS (
    SELECT * FROM {{ ref('int_delivery') }}
    {% if is_incremental() %}
    WHERE scheduled_date > (SELECT MAX(scheduled_date) FROM {{ this }})
       OR write_date      > (SELECT MAX(write_date)     FROM {{ this }})
    {% endif %}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY d.picking_id)   AS id,
    dt.date_id,
    d.partner_id,
    d.sale_id,
    d.location_src_id                           AS location_id,
    d.picking_id,
    d.picking_name,
    d.picking_status,
    d.scheduled_date,
    d.date_done,
    d.delay_days,
    d.is_on_time,
    d.delay_category,                           -- threshold đã fix trong int_delivery

    -- Thêm mới từ V3
    d.qty_ordered,
    d.qty_delivered,
    d.fill_rate_pct,
    d.fill_category,

    d.city,
    d.country_name,
    d.write_date
FROM delivery d
LEFT JOIN dates dt ON dt.date = d.scheduled_date::date