with

revenue as (

  select
    month
    , account_id
    , sum(revenue) as revenue
  from {{ ref('account_monthly_gross_revenue_by_source') }}
  {{ dbt_utils.group_by(n=2) }}

)

, cost as (

  select
    month
    , account_id
    , costs_per_user
  from {{ ref('fct_account_monthly_costs') }}

)

select
  revenue.month
  , revenue.account_id
  , revenue.revenue
  , cost.costs_per_user                   as costs -- noqa: L029
  , revenue.revenue - cost.costs_per_user as margin
from revenue
left join cost on (revenue.account_id = cost.account_id and revenue.month = cost.month)
