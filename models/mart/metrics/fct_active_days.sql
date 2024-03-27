{{ config(materialized="table", dist="account_id", sort="signed_up_at", tags=["daily", "exclude_hourly"], enabled=false) }}

SELECT * FROM {{ ref('active_days_stage') }}