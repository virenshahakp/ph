{{ config(materialized="table", dist="account_id", sort="month", tags=["daily", "exclude_hourly"]) }}

SELECT * FROM {{ ref('account_monthly_gross_revenue_by_source') }}