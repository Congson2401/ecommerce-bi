{{ config(materialized='table') }}

SELECT
    location_id,
    location_name,
    complete_name,
    usage_type,
    is_internal
FROM {{ ref('stg_locations') }}
WHERE active = true