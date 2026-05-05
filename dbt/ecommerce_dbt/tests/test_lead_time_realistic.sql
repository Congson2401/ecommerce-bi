SELECT po_id, lead_time_actual
FROM {{ ref('fact_purchasing') }}
WHERE lead_time_actual IS NOT NULL
  AND (lead_time_actual < 1 OR lead_time_actual > 90)