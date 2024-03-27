with

dates as (

  {{ dbt_utils.date_spine(
      datepart="day"
      , start_date="TO_DATE('2017-11-14', 'yyyy-mm-dd')"
      , end_date="DATEADD(year, 2, current_date)"
      )
  }}

)

, modified_source as (

  select
    date_day                           as observation_date
    , extract(dayofweek from date_day) as day_of_week
    , extract(day from date_day)       as day_of_month
    , extract(dayofyear from date_day) as day_of_year
    , extract(week from date_day)      as week_of_year
    , extract(month from date_day)     as month_of_year
    , extract(year from date_day)      as year --noqa: RF04
    , extract(quarter from date_day)   as quarter --noqa: RF04
    , row_number() over ()             as day_of_philo
  from dates

)

select *
from modified_source