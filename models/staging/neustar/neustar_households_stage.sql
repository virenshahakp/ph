with

households_current as (

  select * from {{ ref('neustar_households_source') }}

)

, households_historical as (

  select * from {{ ref('neustar_households_historical_source') }}
  where user_id not in (select user_id from households_current)

)

select * from households_historical
union all
select * from households_current