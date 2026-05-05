WITH invoices AS (
    SELECT * FROM {{ ref('stg_invoices') }}
    WHERE invoice_state = 'posted'
),

invoice_lines AS (
    SELECT
        invoice_id,
        COUNT(*)            AS line_count,
        SUM(quantity)       AS total_qty
    FROM {{ ref('stg_invoice_lines') }}
    GROUP BY invoice_id
),

payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
    WHERE partner_type = 'customer'
      AND payment_type  = 'inbound'
      AND payment_state = 'posted'   -- ← thêm filter V3
),

invoice_payment_match AS (
    SELECT
        i.invoice_id,
        i.invoice_number,
        i.move_type,
        i.invoice_type_label,
        i.is_refund,
        i.invoice_state,
        i.payment_state,
        i.partner_id,
        i.invoice_date,
        i.amount_untaxed,
        i.amount_tax,
        i.amount_total,
        i.amount_residual,
        i.write_date,

        p.payment_id,
        p.payment_amount,
        p.payment_date,

        CASE
            WHEN p.payment_date IS NOT NULL
             AND i.invoice_date IS NOT NULL
            THEN EXTRACT(DAY FROM (
                p.payment_date::timestamp - i.invoice_date::timestamp
            ))::INTEGER
            ELSE NULL
        END                         AS days_to_payment,

        -- DSO category (thêm mới cho V3)
        CASE
            WHEN p.payment_date IS NULL THEN 'unpaid'
            WHEN EXTRACT(DAY FROM (p.payment_date::timestamp
                - i.invoice_date::timestamp)) = 0  THEN 'same_day'
            WHEN EXTRACT(DAY FROM (p.payment_date::timestamp
                - i.invoice_date::timestamp)) <= 3  THEN '1_3_days'
            WHEN EXTRACT(DAY FROM (p.payment_date::timestamp
                - i.invoice_date::timestamp)) <= 7  THEN '4_7_days'
            WHEN EXTRACT(DAY FROM (p.payment_date::timestamp
                - i.invoice_date::timestamp)) <= 30 THEN '8_30_days'
            ELSE                                         '30_plus_days'
        END                         AS dso_category,

        il.line_count,
        il.total_qty,

        ROW_NUMBER() OVER (
            PARTITION BY i.invoice_id
            ORDER BY p.payment_date ASC NULLS LAST
        )                           AS payment_rank

    FROM invoices i
    LEFT JOIN payments p
        ON  p.partner_id   = i.partner_id
        AND p.payment_date >= i.invoice_date
        AND p.payment_date <= (
            i.invoice_date::timestamp + INTERVAL '30 days'  -- ← giữ 30 ngày, đủ cho installment
        )::date
        AND ABS(p.payment_amount - i.amount_total)
            <= i.amount_total * 0.01  -- ← thêm: match theo amount ±1% tránh match sai
    LEFT JOIN invoice_lines il
        ON il.invoice_id = i.invoice_id
)

SELECT
    invoice_id,
    invoice_number,
    move_type,
    invoice_type_label,
    is_refund,
    invoice_state,
    payment_state,
    partner_id,
    invoice_date,
    amount_untaxed,
    amount_tax,
    amount_total,
    amount_residual,
    payment_id,
    payment_amount,
    payment_date,
    days_to_payment,
    dso_category,       -- thêm mới
    line_count,
    total_qty,
    write_date
FROM invoice_payment_match
WHERE payment_rank = 1