{{ config(
    materialized='tuple_incremental'
    , unique_key=['report_timezone','report_date']
    , sort=['report_timezone'
        , 'report_date'
    ]
    , dist='all'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , on_schema_change = 'append_new_columns'
) }}



{% set end_date = var("end_date") %}
{% set start_date = var("start_date") %}
{% set default_lookback_days = 10 %}

{% if start_date == "" and end_date == "" %}

{% set end_date = run_started_at.astimezone(modules.pytz.timezone("UTC")) %}
{% set start_date = (end_date - modules.datetime.timedelta(default_lookback_days)) %}
{% set end_date = end_date.date() %}
{% set start_date = start_date.date() %}


{% endif %}

--grab everything within 1 day on either side of our start and end date.  
--This is because the query optimizer can't push the dates in the second step down to this step
with predicate_pushdown_hint as (
  select
    date_trunc('Hour', timestamp_start) as report_time 
    , sum(minutes) as watched_minutes
  from {{ ref('fct_watched_minutes') }}
  where timestamp_start::date between date_add('Day', -1, '{{ start_date }}') and date_add('Day', 1, '{{ end_date }}') 
  group by 1
)

, tbl_timezones as (
  select 'US/Pacific' as timezone
  union all
  select 'US/Eastern'
  union all
  select 'US/Mountain'
  union all 
  select 'US/Central'
  union all 
  select 'UTC'
)

select
  tbl_timezones.timezone as report_timezone
  , convert_timezone(tbl_timezones.timezone, predicate_pushdown_hint.report_time)::date as report_date 
  , sum(predicate_pushdown_hint.watched_minutes) as watched_minutes
from predicate_pushdown_hint
cross join tbl_timezones
where convert_timezone(tbl_timezones.timezone, predicate_pushdown_hint.report_time)::date 
  between '{{ start_date }}' and '{{ end_date }}'
group by 1, 2
