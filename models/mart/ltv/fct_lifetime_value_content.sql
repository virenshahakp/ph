{{ config(materialized="table", dist="show_id", sort="watch_month", tags=["daily", "exclude_hourly"], enabled=false) }}

SELECT * FROM {{ ref('ltv_by_show_channel') }}