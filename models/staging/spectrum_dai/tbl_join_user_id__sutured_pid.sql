{{ config(
    materialized='incremental'
    , unique_key=['partition_date', 'sutured_pid' ]
    , sort=['partition_date','sutured_pid']
    , dist='sutured_pid' 
    , tags=["dai", "exclude_hourly", "exclude_daily"]
) }}


{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}



select distinct
  user_id
  , sutured_pid
  , first_value(received_at::date)
    over (partition by user_id, sutured_pid order by received_at rows between unbounded preceding and current row)
  as partition_date
from {{ source('dataserver_prod', 'playback_session_created') }}
where received_at::date between '{{ start_date }}' and '{{ end_date }}'
  and sutured_pid is not null
  {% if is_incremental() %}
    and not exists (
      select 1 as record from {{ this }} as exists_check
      where exists_check.user_id = playback_session_created.user_id
        and exists_check.sutured_pid = playback_session_created.sutured_pid
        and received_at::date
        between '{{ start_date }}'::date - interval '2 day'
        and '{{ start_date }}'::date + interval '2 day'
    )
  {% endif %}
