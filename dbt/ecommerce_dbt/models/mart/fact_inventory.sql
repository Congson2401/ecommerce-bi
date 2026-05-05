{{ config(materialized='incremental', unique_key='id') }}
{%- set _dim_product  = ref('dim_product') -%}
{%- set _dim_location = ref('dim_location') -%}

{% if is_incremental() %}
{%- set max_wd_query -%}
    SELECT COALESCE(MAX(write_date), '1900-01-01')::text AS max_wd FROM {{ this }}
{%- endset -%}
{%- set max_wd = run_query(max_wd_query).columns[0].values()[0] -%}
{% endif %}

WITH inventory AS (
    SELECT * FROM {{ ref('int_inventory_moves') }}
    {% if is_incremental() %}
    WHERE write_date > '{{ max_wd }}'
    {% endif %}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY i.move_id)      AS id,
    d.date_id,
    i.product_id,
    i.location_dest_id                          AS location_id,
    i.picking_id,
    i.picking_type_id,
    i.sale_line_id,
    i.purchase_line_id,
    i.move_type,
    i.move_status,

    i.qty_in,
    i.qty_out,
    i.qty_ordered,

    CASE
        WHEN i.move_type = 'out' THEN i.qty_out
        WHEN i.move_type = 'in'  THEN i.qty_in
        ELSE 0
    END                                         AS qty_delivered,

    CASE
        WHEN i.move_type = 'out'
         AND COALESCE(i.qty_ordered, 0) > 0
        THEN ROUND(i.qty_out::numeric / i.qty_ordered * 100, 2)
        ELSE NULL
    END                                         AS fill_rate_pct,

    i.qty_on_hand,
    i.qty_reserved,
    i.qty_available,

    -- Giá trị hàng di chuyển tại thời điểm move (in hoặc out)
    -- = qty_delivered * đơn giá, ước tính từ running_inventory_value / running_stock
    CASE
        WHEN GREATEST(i.running_stock, 0) > 0
        THEN ROUND(
            GREATEST(i.running_inventory_value, 0)
            / GREATEST(i.running_stock, 0)
            * CASE
                WHEN i.move_type = 'in'  THEN i.qty_in
                WHEN i.move_type = 'out' THEN i.qty_out
                ELSE 0
              END
        , 2)
        ELSE 0
    END                                         AS inventory_value,

    -- Tổng giá trị tồn kho hiện tại sau move này
    GREATEST(i.running_stock, 0)                AS running_stock,
    GREATEST(i.running_inventory_value, 0)      AS running_inventory_value,

    i.write_date
FROM inventory i
LEFT JOIN dates d ON d.date = i.move_date::date