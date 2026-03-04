-- Single Trip/Local30/...
{{ config(materialized='table') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY subscriber_type) AS rider_type_key,
    subscriber_type,
    CASE 
        WHEN LOWER(subscriber_type) LIKE '%annual%' THEN 'member'
        ELSE 'casual'
    END AS rider_category
FROM (
    SELECT DISTINCT subscriber_type
    FROM {{ ref('stg_bikeshare_trips') }}
)