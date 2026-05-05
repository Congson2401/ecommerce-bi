WITH source AS (
    SELECT * FROM {{ source('public', 'stock_location') }}
),

cleaned AS (
    SELECT
        id              AS location_id,
        name            AS location_name,
        complete_name,
        usage           AS usage_type,
        CASE
            WHEN usage = 'internal' THEN true
            ELSE false
        END             AS is_internal,
        active
    FROM source
    WHERE active = true
)

SELECT * FROM cleaned