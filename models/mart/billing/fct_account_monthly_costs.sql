{{ config(materialized="table", dist="account_id", sort="month") }}

SELECT 
  month::DATE AS month -- noqa: L029
  , account_id
  , COALESCE(SUM(backend_costs), 0) AS costs_per_user
FROM {{ ref('account_monthly_costs_stage') }}
{{ dbt_utils.group_by(n=2) }}