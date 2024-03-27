with

daily_spend as (

  select * from {{ ref('airbyte_daily_spend_source') }}

)

select * from daily_spend
