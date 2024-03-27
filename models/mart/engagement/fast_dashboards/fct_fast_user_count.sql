{{ config(
  materialized='tuple_incremental'
  , unique_key=['report_date']
  , sort=['report_date']
  , on_schema_change = 'append_new_columns'
  , tags=["daily", "exclude_hourly"]
) }}

{% set dates = get_update_dates(4) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with dates as (
  select * from {{ ref('dim_dates') }}
)

, fast as (
  select * from {{ ref('dim_fast_access_range') }}
)


select
  fast.account_id
  , dates.observation_date::date as report_date
from fast
inner join
  dates on
    dates.observation_date between convert_timezone(
      'US/Pacific', fast.fast_start_time
    )::date and convert_timezone('US/Pacific', fast.end_time)::date
where dates.observation_date between '{{ start_date }}' and '{{ end_date }}'
