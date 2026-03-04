WITH stations AS (

    SELECT 
        start_station_id AS station_id,
        start_station_name AS station_name,
        start_time AS observed_at
    FROM {{ ref('stg_bikeshare_trips') }}

    UNION ALL

    SELECT 
        end_station_id,
        end_station_name,
        TIMESTAMP_ADD(
            start_time, 
            -- INTERVAL CAST(duration_minutes AS INT64) MINUTE
            INTERVAL COALESCE(CAST(duration_minutes AS INT64), 0) MINUTE
        ) AS observed_at
    FROM {{ ref('stg_bikeshare_trips') }}
)
SELECT
    station_id,
    station_name,
    MAX(observed_at) AS last_seen_at
FROM stations
WHERE station_id IS NOT NULL
GROUP BY station_id, station_name