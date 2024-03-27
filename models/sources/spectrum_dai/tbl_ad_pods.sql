{{ config(
    materialized='tuple_incremental'
    , sort=['partition_date', 'player_pod_id']
    , dist='player_pod_id'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , unique_key = ['partition_date']
    , on_schema_change = 'append_new_columns'
) }}


{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

--noqa: disable=L034

with assign_partition_dates as (
  select
    (year || '-' || month || '-' || day)::date                 as partition_date_alias
    , first_value(partition_date_alias) over (
      partition by player_pod_id
      order by coalesce(created_at, sent_at) rows between unbounded preceding and current row
    )                                                          as first_partition_date
    , first_value(hour) over (
      partition by player_pod_id
      order by coalesce(created_at, sent_at) rows between unbounded preceding and current row
    )                                                          as first_partition_hour
    , asset_type                                               as asset_type
    , channel                                                  as channel
    , client_type                                              as client_type
    , duration_mismatch_ads                                    as duration_mismatch_ads
    , inactive_ads                                             as inactive_ads
    , fallback_ads                                             as fallback_ads
    , house_ads                                                as house_ads
    , latency_ms                                               as latency_ms
    , network                                                  as network
    , player_id                                                as sutured_pid
    , pod_id                                                   as pod_id
    , request_id                                               as request_id
    , track                                                    as track
    , uningested_ads                                           as uningested_ads
    , url_dup_ads                                              as url_dup_ads
    , version                                                  as manifest_system_version
    , has_uid_token                                            as has_uid_token
    , has_aip_token                                            as has_aip_token
    , extra_ads
    , inserted_extra_ads
    , inserted_extra_ads_duration
    , pod_id || ':' || player_id                               as player_pod_id
    , {{ pod_instance_id(source = 'ad_pods') }}           as pod_instance_id
    , case
      when version is null
        then sent_at
      else created_at
    end                                                        as event_timestamp

    , dateadd('ms', latency_ms, coalesce(created_at, sent_at)) as received_at
    , case
      when version is null
        then duplicate_ads
      else dup_ads
    end                                                        as duplicate_ads
    , case
      when version is null
        then duration::float
      else request_duration::float
    end                                                        as duration
    , case
      when version is null
        then fill_duration
      else inserted_paid_ads_duration
    end                                                        as fill_duration
    , case
      when version is null
        then fingerprint_dup_ads
      else audio_dup_ads
    end                                                        as fingerprint_dup_ads
    , case
      when version is null
        then frequency_capped_ads
      else fcapped_ads
    end                                                        as frequency_capped_ads
    , case
      when version is null
        then house_duration
      else inserted_house_ads_duration
    end                                                        as house_duration
    , case
      when version is null
        then null
      else ingested_ads
    end                                                        as ingested_ads
    , case
      when version is null
        then inserted_ads
      else inserted_paid_ads
    end                                                        as inserted_ads
    , case
      when version is null
        then null
      else inserted_house_ads_duration
    end                                                        as inserted_house_ads_duration
    , case
      when version is null
        then null
      else inserted_house_ads
    end                                                        as inserted_house_ads
    , case
      when version is null
        then null
      else inserted_paid_ads_duration
    end                                                        as inserted_paid_ads_duration
    , case
      when version is null
        then null
      else inserted_paid_ads
    end                                                        as inserted_paid_ads
    , case
      when version is null
        then null
      else inserted_philler_ads_duration
    end                                                        as inserted_philler_ads_duration
    , case
      when version is null
        then null
      else inserted_philler_ads
    end                                                        as inserted_philler_ads
    , case
      when version is null
        then null
      else is_live_edge
    end                                                        as is_live_edge
    , case
      when version is null
        then owner
      else pod_owner
    end                                                        as owner --noqa: L029
    , case
      when version is null
        then primary_ads
    end                                                        as primary_ads
    , case
      when version is null
        then null
      else program_offset
    end                                                        as program_offset
    , case
      when version is null
        then null
      else received_ads
    end                                                        as received_ads
    , case
      when version is null
        then status
      when lower(status) = 'ok'
        and request_duration - inserted_paid_ads_duration between -4 and 4
        then 'ok'
      when lower(status) = 'ok'
        and request_duration - inserted_paid_ads_duration > 4
        then 'underfill'
      when lower(status) in ('fill timed out', 'ad selection algorithm timeout', 'unfilled ad pod')
        then 'empty'
      when lower(status) = 'unsaved ad pod'
        then 'duplicate_pod'
    end                                                        as status
    , lower(status)                                            as stitcher_status
    , case
      when version is null
        then 'sutured'
      else 'stitcher'
    end                                                        as manifest_system
  from {{ source('spectrum_dai', 'ad_pods') }}
  where partition_date_alias between '{{ start_date }}'::date - interval '1 day'
    and '{{ end_date }}'::date + interval '1 day'
)

select
  first_partition_date                                                           as partition_date
  , first_partition_hour                                                         as partition_hour
  , date_add('hour', first_partition_hour::int, first_partition_date::timestamp) as partition_date_hour
  , asset_type
  , channel
  , client_type
  , duration_mismatch_ads
  , inactive_ads
  , fallback_ads
  , house_ads
  , latency_ms
  , network
  , pod_id
  , request_id
  , track
  , uningested_ads
  , url_dup_ads
  , manifest_system_version
  , player_pod_id
  , event_timestamp
  , received_at
  , duplicate_ads
  , duration
  , fill_duration
  , fingerprint_dup_ads
  , frequency_capped_ads
  , house_duration
  , ingested_ads
  , inserted_ads
  , inserted_house_ads_duration
  , inserted_house_ads
  , inserted_paid_ads_duration
  , inserted_paid_ads
  , coalesce(inserted_philler_ads_duration, 0)                                   as inserted_philler_ads_duration
  , coalesce(inserted_philler_ads, 0)                                            as inserted_philler_ads
  , is_live_edge
  , owner
  , primary_ads
  , program_offset
  , received_ads
  , status
  , stitcher_status
  , manifest_system
  , sutured_pid
  , pod_instance_id
  , has_uid_token
  , has_aip_token
  , row_number() over (
    partition by player_pod_id order by case when lower(status) = 'ok' then 0 else 1 end
  )                                                                              as dedupe_number
from assign_partition_dates
where partition_date between '{{ start_date }}' and '{{ end_date }}'
qualify row_number() over (partition by pod_instance_id order by received_at) = 1