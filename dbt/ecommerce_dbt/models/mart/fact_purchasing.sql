{{ config(materialized='incremental', unique_key='id') }}
{%- set _dim_product = ref('dim_product') -%}
{%- set _dim_partner = ref('dim_partner') -%}
{% if is_incremental() %}
{%- set max_wd_query -%}
    SELECT COALESCE(MAX(write_date), '1900-01-01')::text AS max_wd FROM {{ this }}
{%- endset -%}
{%- set max_wd = run_query(max_wd_query).columns[0].values()[0] -%}
{% endif %}

WITH purchasing AS (
    SELECT * FROM {{ ref('int_purchasing') }}
    {% if is_incremental() %}
    WHERE write_date > '{{ max_wd }}'
    {% endif %}
),
dates AS (
    SELECT * FROM {{ ref('dim_date') }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY p.pol_id)   AS id,
    d.date_id,
    p.product_id,
    p.supplier_id,
    p.po_id,
    p.po_name,
    p.po_status,
    p.picking_type_id,
    p.picking_id,
    p.picking_name,
    p.picking_status,
    p.picking_scheduled_date,
    p.picking_date_done,
    p.picking_delay_days,
    p.picking_is_on_time,
    p.qty_ordered,
    p.qty_received,
    p.price_unit,
    p.purchase_amount,
    p.receipt_rate_pct,
    p.lead_time_actual,
    p.lead_time_planned,
    p.lead_time_variance,
    p.is_on_time,
    p.price_agreed,
    p.lead_time_committed,
    p.is_price_compliant,
    p.write_date
FROM purchasing p
LEFT JOIN dates d ON d.date = p.order_date::date