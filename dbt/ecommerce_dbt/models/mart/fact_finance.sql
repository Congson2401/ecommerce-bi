-- fact_finance.sql
-- Fact table for invoices, payments, and financial metrics
-- Grain: one row per invoice

{{ config(
    materialized='incremental',
    unique_key='id'
) }}
{%- set _dim_partner = ref('dim_partner') -%}
{%- set _dim_payment_method = ref('dim_payment_method') -%}
WITH finance AS (
    SELECT * FROM {{ ref('int_finance') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

-- Assign payment method based on distribution
-- (V2 generator doesn't store method in DB, so we approximate by hash)
payment_method_assign AS (
    SELECT
        f.*,
        CASE
            WHEN f.payment_id IS NULL THEN NULL
            WHEN MOD(f.payment_id, 100) < 25  THEN 1  -- cod
            WHEN MOD(f.payment_id, 100) < 45  THEN 2  -- bank_transfer
            WHEN MOD(f.payment_id, 100) < 63  THEN 3  -- momo
            WHEN MOD(f.payment_id, 100) < 75  THEN 4  -- zalopay
            WHEN MOD(f.payment_id, 100) < 85  THEN 5  -- vnpay
            WHEN MOD(f.payment_id, 100) < 93  THEN 6  -- credit_card
            WHEN MOD(f.payment_id, 100) < 98  THEN 7  -- shopee_pay
            ELSE 8                                      -- installment
        END AS method_id
    FROM finance f
)

SELECT
    ROW_NUMBER() OVER (ORDER BY pma.invoice_id) AS id,
    dd.date_id,
    pma.partner_id,
    pma.method_id,
    pma.invoice_id,
    pma.invoice_number,
    pma.invoice_type_label              AS invoice_type,
    pma.invoice_state,
    pma.payment_state,
    pma.amount_untaxed,
    pma.amount_tax,
    pma.amount_total,
    pma.amount_residual,
    COALESCE(pma.payment_amount, 0)     AS payment_amount,
    pma.payment_date,
    pma.days_to_payment,
    pma.write_date

FROM payment_method_assign pma
LEFT JOIN dim_date dd
    ON dd.date = pma.invoice_date

{% if is_incremental() %}
WHERE pma.write_date > (SELECT COALESCE(MAX(write_date), '1900-01-01') FROM {{ this }})
{% endif %}
