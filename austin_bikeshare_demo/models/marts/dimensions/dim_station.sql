-- id/name/latlong

{{ config(materialized='table') }}

SELECT
    {{ dbt_utils.generate_surrogate_key(['station_id','dbt_valid_from']) }} AS station_key,
    station_id,
    station_name,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to
FROM {{ ref('station_snapshot') }}