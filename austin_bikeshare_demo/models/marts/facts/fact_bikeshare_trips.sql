{{ config(
    materialized='table',
    partition_by={
        "field": "trip_date",
        "data_type": "date"
    },
    cluster_by=["start_station_key","end_station_key","rider_type_key"]
) }}

WITH trips AS (

    SELECT *
    FROM {{ ref('stg_bikeshare_trips') }}

    WHERE trip_id IS NOT NULL
      AND start_time IS NOT NULL
      AND end_time IS NOT NULL

),

-- Join to date dimension
date_join AS (

    SELECT
        t.*,
        d.date_key
    FROM trips t
    LEFT JOIN {{ ref('dim_date') }} d
        ON DATE(t.start_time) = d.date_key

),

-- Join start station using SCD2 logic
start_station_join AS (

    SELECT
        dj.*,
        ss.station_key AS start_station_key
    FROM date_join dj
    LEFT JOIN {{ ref('dim_station') }} ss
        ON dj.start_station_id = ss.station_id
        AND dj.start_time >= ss.valid_from
        AND (
              dj.start_time < ss.valid_to
              OR ss.valid_to IS NULL
            )

),

-- Join end station using SCD2 logic
end_station_join AS (

    SELECT
        ssj.*,
        es.station_key AS end_station_key
    FROM start_station_join ssj
    LEFT JOIN {{ ref('dim_station') }} es
        ON ssj.end_station_id = es.station_id
        AND ssj.end_time >= es.valid_from
        AND (
              ssj.end_time < es.valid_to
              OR es.valid_to IS NULL
            )

),

-- Join rider type
final_join AS (

    SELECT
        esj.*,
        r.rider_type_key
    FROM end_station_join esj
    LEFT JOIN {{ ref('dim_rider_type') }} r
        ON esj.subscriber_type = r.subscriber_type

)

SELECT

    -- Primary Key (degenerate dimension)
    trip_id,

    -- Foreign Keys
    date_key,
    start_station_key,
    end_station_key,
    rider_type_key,

    -- Degenerate dimension
    bike_id,

    -- Time attributes
    start_time,
    end_time,
    DATE(start_time) AS trip_date,

    -- Measures
    duration_minutes,
    SAFE_DIVIDE(duration_minutes, 60) AS duration_hours,
    1 AS trip_count,

    -- Data quality helper
    CURRENT_TIMESTAMP() AS loaded_at

FROM final_join