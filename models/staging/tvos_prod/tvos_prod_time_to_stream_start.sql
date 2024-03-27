{{
  config(
    materialized='tuple_incremental'
    , dist='user_id'
    , unique_key=['open_date']
    , sort=['open_time']
    , tags=['exclude_hourly']
  )
}}


{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with

all_platform_stream_starts as (

  select
    user_id
    , platform
    , event_timestamp       as stream_start_at
    , event_timestamp::date as stream_start_date
  from {{ ref('fct_stream_starts') }}
  where
    event_timestamp between '{{ start_date }}' and '{{ end_date }}'
    and platform = 'tvos'

)

, tvos_open as (

  select
    user_id
    , visited_at       as open_time
    , visited_at::date as open_date
  from {{ ref('tvos_prod_application_opened_stage') }}
  where
    visited_at between '{{ start_date }}' and '{{ end_date }}'

)

, stream_starts as (

  select
    user_id
    , platform
    , stream_start_date
    , stream_start_at
  from all_platform_stream_starts

)

, time_diff as (

  select
    tvos_open.user_id
    , stream_starts.platform
    , tvos_open.open_date
    , stream_starts.stream_start_at
    , tvos_open.open_time
    , datediff('seconds', tvos_open.open_time, stream_starts.stream_start_at) as time_diff_sec
  from tvos_open
  join stream_starts
    on tvos_open.user_id = stream_starts.user_id
      and tvos_open.open_date = stream_starts.stream_start_date
  where -- Ensure that we only consider the first 10 mins after application opened
    datediff('seconds', tvos_open.open_time, stream_starts.stream_start_at) > 1
    and datediff('seconds', tvos_open.open_time, stream_starts.stream_start_at) < 600
  order by
    tvos_open.user_id
    , stream_starts.stream_start_at

)

, min_time_diff as (

  select
    user_id
    , open_date
    , open_time
    , min(time_diff_sec) as min_time_diff_sec
  from time_diff
  {{ dbt_utils.group_by(n=3) }}

)

, first_stream_start as (

  select
    time_diff.user_id
    , time_diff.platform
    , time_diff.open_date
    , time_diff.stream_start_at
    , time_diff.open_time
    , min_time_diff.min_time_diff_sec
  from time_diff
  join min_time_diff on time_diff.user_id = min_time_diff.user_id
    and time_diff.time_diff_sec = min_time_diff.min_time_diff_sec
    and time_diff.open_date = min_time_diff.open_date

)

, row_ordering as (

  select
    user_id
    , platform
    , open_date
    , stream_start_at
    , open_time
    , min_time_diff_sec
    , row_number()
      over (
        partition by
          user_id
          , open_date
          , stream_start_at
        order by
          open_time desc
      )
    as row_num
  from first_stream_start
  where 1 = 1
  qualify row_num = 1

)

select
  platform
  , user_id
  , open_time                    as open_time
  , min_time_diff_sec            as time_to_stream_start
  , date_trunc('day', open_date) as open_date
from row_ordering
