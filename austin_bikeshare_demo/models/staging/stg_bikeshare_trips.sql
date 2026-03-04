{{ config(materialized='view') }}

SELECT
    cast(trip_id as STRING) as trip_id,
    subscriber_type,
    cast(bike_id as STRING) as bike_id,
    bike_type,
    start_time,
    cast(start_station_id as STRING) as start_station_id,
    start_station_name,
    cast(end_station_id as STRING) as end_station_id,
    end_station_name,
    cast(duration_minutes as INT64) as duration_minutes
FROM {{ source('austin_bikeshare', 'bikeshare_trips') }}
-- FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`