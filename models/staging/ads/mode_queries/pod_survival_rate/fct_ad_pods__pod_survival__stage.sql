{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
    'partition_date'
    , 'platform'
    , 'asset_type'
    , 'network'
    , 'channel'
    , 'ad_server'
  ]
  , dist='platform'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with pod_data as (
  select
    *
    , case when status = 'empty' then 1 else 0 end          as is_empty
    , case when status = 'underfill' then 1 else 0 end      as is_underfill
    , case when status = 'ok' then 1 else 0 end             as is_ok
    , case when status = 'ok' then fill_duration end        as is_ok__fill_duration
    , case when status = 'empty' then fill_duration end     as is_empty__fill_duration
    , case when status = 'underfill' then fill_duration end as is_underfill__fill_duration
    , case
      when
        (lower(network) in ('discovery', 'scripps') and lower(asset_type) in ('live', 'dvr')) then 'freewheel'
      else 'publica'
    end                                                     as ad_server
  from {{ ref('tbl_ad_pods') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, pods as (
  select
    partition_date
    , client_type                      as platform
    , network
    , channel
    , asset_type
    , sutured_pid
    , ad_server
    , pod_id || ':' || sutured_pid     as pod_instance --TODO: DEV-15587 use standard pod_instance_id macro
    , max(is_empty)                    as is_empty
    , max(is_underfill)                as is_underfill
    , max(is_ok)                       as is_ok
    , count(1)                         as cnt_requests
    , sum(is_empty)                    as cnt_empty_requests
    , sum(is_underfill)                as cnt_underfill_requests
    , sum(is_ok)                       as cnt_ok_requests
    , max(duration)                    as pod_duration
    , max(is_ok__fill_duration)        as is_ok__fill_duration
    , max(is_underfill__fill_duration) as is_underfill__fill_duration
    , max(is_empty__fill_duration)     as is_empty__fill_duration
  from pod_data
  group by 1, 2, 3, 4, 5, 6, 7, 8
)

select
  partition_date
  , platform
  , asset_type
  , network
  , channel
  , ad_server
  , count(distinct sutured_pid)      as user_count
  , sum(pod_duration)                as pod_duration
  , sum(is_ok__fill_duration)        as is_ok__fill_duration
  , sum(is_underfill__fill_duration) as is_underfill__fill_duration
  , sum(is_empty__fill_duration)     as is_empty__fill_duration
  , count(1)                         as pod_count
  , sum(is_ok)                       as ad_pods_status_ok_pod_count
  , sum(is_underfill)                as ad_pods_status_underfill_pod_count
  , sum(is_empty)                    as ad_pods_status_empty_pod_count
  , sum(cnt_requests)                as requests_count
  , sum(cnt_empty_requests)          as empty_requests_count
  , sum(cnt_underfill_requests)      as underfill_requests_count
  , sum(cnt_ok_requests)             as ok_requests_count
from pods
group by 1, 2, 3, 4, 5, 6
order by 1, 2, 3, 4, 5, 6

