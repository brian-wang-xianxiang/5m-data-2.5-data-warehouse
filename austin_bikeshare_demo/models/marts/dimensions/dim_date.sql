-- date/day/week/month/quarter/year/weekday
{{ config(materialized='table') }}

WITH dates AS (

    SELECT DISTINCT DATE(start_time) AS date_day
    FROM {{ ref('stg_bikeshare_trips') }}

)

SELECT
    date_day AS date_key,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    FORMAT_DATE('%A', date_day) AS day_name,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1,7) THEN TRUE
        ELSE FALSE
    END AS is_weekend
FROM dates