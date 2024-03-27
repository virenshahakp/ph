{{ config(materialized="table", dist="genre_id", sort="watch_month", tags=["daily", "exclude_hourly"], enabled=false) }}

SELECT * FROM {{ ref('ltv_by_genre_stage') }}