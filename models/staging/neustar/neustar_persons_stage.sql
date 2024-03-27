with

neustar_persons_current as (

  select * from {{ ref('neustar_persons_source') }}

)

, neustar_persons_historical as (

  select * from {{ ref('neustar_persons_historical_source') }}
  where user_id not in (select user_id from neustar_persons_current)

)

, neustar_persons as (

  select * from neustar_persons_current
  union all
  select * from neustar_persons_historical

)

, grouped as (

  select
    *
    , (10 * floor((datepart(year, current_date) - birth_year) / 10.0))::char(3) || 's' as age_range
  from neustar_persons

)

select * from grouped
