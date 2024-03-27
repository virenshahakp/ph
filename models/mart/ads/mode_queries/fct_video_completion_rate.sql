{{ config(
    materialized='tuple_incremental'
    , unique_key=['partition_date_hour']
    , sort=[
      'partition_date_hour'
      , 'client_type'
      , 'asset_type'
      , 'os_version'
      , 'app_version'
    ]
    , dist='app_version' 
    , tags=["dai", "exclude_hourly", "exclude_daily"]
    , on_schema_change = 'append_new_columns'
) }}


{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with fct_beacons as (
  select *                         
  from {{ ref('fct_beacons') }}
  where 
    partition_date between '{{ start_date }}' and '{{ end_date }}'  --noqa: LT05
    and manifest_system is not null
)

, playback_session_key as (
  select 
    sutured_pid
    , os_version
    , app_version
  from {{ ref('fct_playback_sessions') }}
  where 
    created_at between '{{ start_date }}' and '{{ end_date }}'      --noqa: LT05
  {{ dbt_utils.group_by(n=3) }}
)

select                                                              --noqa: L034
  dateadd(
    'hour'
    , fct_beacons.partition_hour::int
    , fct_beacons.partition_date::date
  )                                                     as partition_date_hour
  , fct_beacons.client_type
  , fct_beacons.asset_type
  , playback_session_key.os_version
  , playback_session_key.app_version
  , sum(
    case 
      when fct_beacons.beacon_type = 'impression' 
        then 1 
    end
  )                                                     as impression_count
  , sum(
    case 
      when fct_beacons.beacon_type = 'complete' 
        then 1 
    end
  )                                                     as completion_count
from fct_beacons
left join playback_session_key
  on fct_beacons.sutured_pid = playback_session_key.sutured_pid
{{ dbt_utils.group_by(n=5) }}