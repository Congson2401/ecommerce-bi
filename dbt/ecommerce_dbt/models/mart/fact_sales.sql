{{ config(materialized='incremental', unique_key='id') }}
{%- set _dim_product = ref('dim_product') -%}
{%- set _dim_partner = ref('dim_partner') -%}
{% if is_incremental() %}
{%- set max_wd_query -%}
    SELECT COALESCE(MAX(write_date), '1900-01-01')::text AS max_wd FROM {{ this }}
{%- endset -%}
{%- set max_wd = run_query(max_wd_query).columns[0].values()[0] -%}
{% endif %}

WITH sales AS (
    SELECT * FROM {{ ref('int_sales') }}
    {% if is_incremental() %}
    WHERE write_date > '{{ max_wd }}'
    {% endif %}
),
dates AS (
    SELECT * FROM {{ ref('dim_date') }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY s.line_id)  AS id,
    d.date_id,
    s.product_id,
    s.customer_id,
    s.order_id,
    s.order_name,
    s.order_status,
    s.qty_sold,
    s.price_unit,
    s.discount,
    s.revenue,
    s.cost,
    s.gross_margin,
    s.gross_margin_pct,
    s.write_date
FROM sales s
LEFT JOIN dates d ON d.date = s.order_date::date