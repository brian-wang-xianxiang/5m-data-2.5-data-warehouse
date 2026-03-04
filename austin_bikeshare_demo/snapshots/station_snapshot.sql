{% snapshot station_snapshot %}

{{
  config(
    target_schema='snapshots',
    unique_key='station_id',
    strategy='check',
    check_cols=['station_name']
  )
}}

SELECT
    station_id,
    station_name,
    last_seen_at
FROM {{ ref('stg_stations') }}

{% endsnapshot %}