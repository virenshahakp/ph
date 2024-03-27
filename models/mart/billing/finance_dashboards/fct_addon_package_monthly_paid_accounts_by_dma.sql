{{ config(materialized="view") }}

with

daily_totals as (

  select * from {{ ref('fct_addon_package_daily_paid_accounts_by_dma') }}

)

select
  package
  , dma_sort
  , dma_code
  , dma_name
  , {{ dbt_utils.pivot(
    column='day_of'
    , values=dbt_utils.get_column_values(
        table=ref('dim_dates')
        , column='observation_date::date'
        , where="day_of_month = 1 and observation_date between '2021-08-01'::date and current_date"
    )
    , agg='sum'
    , then_value='active_paid_total'
    , else_value=0
  ) }}
from daily_totals
{{ dbt_utils.group_by(n=4) }}
order by 1 asc, 2 asc, 3 asc, 4 asc
