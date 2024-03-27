{{ 
  config(
    materialized='tuple_incremental'
    , sort=['partition_date','pod_instance_id']
    , dist='pod_instance_id'
    , tags=["dai", "exclude_hourly", "exclude_daily"]
    , unique_key = ['partition_date']
    , on_schema_change = 'append_new_columns'
  ) 
}}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

/*
This model is used in experimentation to understand the impact on
live ad pod duration deficits of various experiments
To be depricated with https://philoinc.atlassian.net/browse/DEV-14487
*/

with

live_pod_instances as (

  select
    partition_date
    , partition_date_hour
    , user_id
    , stitcher_status
    , asset_type
    , sutured_pid
    , pod_id
    , request_id
    , replace(pod_duration_seconds, 's', '')::int as pod_duration_seconds
    , pod_instance_id
    , sum(duration)                               as total_vast_ad_duration
    , min(duration)                               as min_vast_ad_duration
    , sum(
      case
        when cpm_price > 1 then (cpm_price / 1000.0)
        else 0
      end
    )                                             as revenue
    , sum(1)                                      as count_ads
    , sum(
      case
        when is_evergreen is true then 1
        else 0
      end
    )                                             as count_evergreen_ads
  from {{ ref('fct_vast_ads_enriched') }}
  where is_viable is true
    and is_fallback is false
    and partition_date between '{{ start_date }}' and '{{ end_date }}'
  {{ dbt_utils.group_by(n=10) }}

)

select
  partition_date
  , partition_date_hour
  , user_id
  , pod_instance_id::varchar(500)
  , stitcher_status
  , asset_type
  , pod_duration_seconds
  , total_vast_ad_duration
  , min_vast_ad_duration
  , revenue
  , count_ads
  , count_evergreen_ads
  , sutured_pid
  , pod_id
  , request_id
  , case
    when count_ads = count_evergreen_ads then '0: all evergreen'
    when pod_duration_seconds < min_vast_ad_duration then '1: ads too long'
    when (pod_duration_seconds - total_vast_ad_duration) < -4 then 'A: < -4s'
    when (pod_duration_seconds - total_vast_ad_duration) <= 4 then 'B: -4s:4s'
    when (pod_duration_seconds - total_vast_ad_duration) <= 33 then 'C: 5s:33s'
    when (pod_duration_seconds - total_vast_ad_duration) <= 90 then 'D: 34s:90s'
    when (pod_duration_seconds - total_vast_ad_duration) <= 120 then 'E: 91s:120s'
    when (pod_duration_seconds - total_vast_ad_duration) <= 180 then 'F: 121s:180s'
    when (pod_duration_seconds - total_vast_ad_duration) > 180 then 'G: 180s+'
    else 'undefined'
  end                                               as deficit_duration_buckets
  , (pod_duration_seconds - total_vast_ad_duration) as deficit_duration_value
from live_pod_instances
