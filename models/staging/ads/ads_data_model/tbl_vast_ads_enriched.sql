{{ 
  config(
    materialized='tuple_incremental'
    , sort=['partition_date', 'partition_date_hour', 'player_pod_id']
    , dist='player_pod_id'
    , tags=["dai", "exclude_hourly", "exclude_daily"]
    , unique_key = ['partition_date']
    , on_schema_change = 'append_new_columns'
  )
}}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with tbl_vast_ads_beacons as (
  select
    tbl_vast_ads_beacons.partition_date
    , tbl_vast_ads_beacons.partition_date_hour
    , tbl_vast_ads_beacons.sent_at
    , tbl_vast_ads_beacons.player_pod_id
    , tbl_vast_ads_beacons.sutured_pid
    , tbl_vast_ads_beacons.pod_id
    , tbl_vast_ads_beacons.request_id
    , tbl_vast_ads_beacons.ad_ident
    , tbl_vast_ads_beacons.creative_id
    , tbl_vast_ads_beacons.dup_ident
    , tbl_vast_ads_beacons.provider_ident
    , tbl_vast_ads_beacons.ad_reseller
    , tbl_vast_ads_beacons.ad_system
    , tbl_vast_ads_beacons.cpm_currency
    , tbl_vast_ads_beacons.cpm_price
    , tbl_vast_ads_beacons.duration
    , tbl_vast_ads_beacons.has_duration_mismatch
    , tbl_vast_ads_beacons.invalid_beacon_count
    , tbl_vast_ads_beacons.invalid_tracking_events
    , tbl_vast_ads_beacons.is_active
    , tbl_vast_ads_beacons.is_dup
    , tbl_vast_ads_beacons.is_empty
    , tbl_vast_ads_beacons.is_evergreen
    , tbl_vast_ads_beacons.is_fallback
    , tbl_vast_ads_beacons.is_fcapped
    , tbl_vast_ads_beacons.is_filled
    , tbl_vast_ads_beacons.is_fingerprint_dup
    , tbl_vast_ads_beacons.is_ingested
    , tbl_vast_ads_beacons.is_inserted
    , tbl_vast_ads_beacons.is_url_dup
    , tbl_vast_ads_beacons.is_philler
    , tbl_vast_ads_beacons.vast_depth
    , tbl_vast_ads_beacons.vast_latency_ms
    , tbl_vast_ads_beacons.vast_position
    , tbl_vast_ads_beacons.vast_status
    , tbl_vast_ads_beacons.vast_version
    , tbl_vast_ads_beacons.manifest_system
    , tbl_vast_ads_beacons.manifest_system_version
    , tbl_ad_pods.asset_type
    , tbl_ad_pods.client_type
    , tbl_ad_pods.network
    , tbl_ad_pods.channel
    , tbl_ad_pods.is_live_edge
    , tbl_ad_pods.owner
    , tbl_ad_pods.stitcher_status
    , true                                  as has_vast_record
    , tbl_vast_ads_beacons.pod_instance_id
    , tbl_vast_ads_beacons.beacon_vast_difference_seconds
    , tbl_vast_ads_beacons.media_url
    , tbl_vast_ads_beacons.media_url_hash
    , tbl_vast_ads_beacons.has_url_mismatch
    , tbl_ad_pods.has_uid_token
    , tbl_ad_pods.has_aip_token
    , tbl_vast_ads_beacons.is_extra
    , tbl_vast_ads_beacons.is_expired
    , case
      when tbl_ad_pods.stitcher_status = 'unsaved_ad_pod' then 0 else tbl_vast_ads_beacons.impression_count
    end                                     as impression_count
    , case
      when tbl_ad_pods.stitcher_status = 'unsaved_ad_pod' then 0 else tbl_vast_ads_beacons.complete_count
    end                                     as complete_count
    , nullif(
      tbl_vast_ads_beacons.impression_count, 0
    ) is not null                           as has_beacon_record
    , tbl_ad_pods.player_pod_id is not null as has_ad_pod_record
    , round(tbl_ad_pods.duration)::varchar
    || 's'                                  as pod_duration_seconds
  from {{ ref('tbl_vast_ads_beacons') }} as tbl_vast_ads_beacons
  left join {{ ref('tbl_ad_pods') }}
    on
      tbl_ad_pods.player_pod_id = tbl_vast_ads_beacons.player_pod_id
      and tbl_ad_pods.request_id = tbl_vast_ads_beacons.request_id
      and tbl_ad_pods.partition_date between (
        '{{ start_date }}'::date - interval '1 day'
      ) and (
        '{{ end_date }}'::date + interval '1 day'
      )
  where
    tbl_vast_ads_beacons.partition_date between
    '{{ start_date }}' and '{{ end_date }}'

)

, tbl_missing_vast_records as (
  select
    tbl_missing_vast_records.partition_date
    , tbl_missing_vast_records.partition_date_hour
    , null                                                       as sent_at
    , tbl_missing_vast_records.player_pod_id
    , split_part(tbl_missing_vast_records.player_pod_id, ':', 2) as sutured_pid
    , split_part(tbl_missing_vast_records.player_pod_id, ':', 1) as pod_id
    , tbl_ad_pods.request_id
    , null                                                       as ad_ident
    , tbl_missing_vast_records.creative_id
    , null                                                       as dup_ident
    , null                                                       as provider_ident
    , null                                                       as ad_reseller
    , tbl_missing_vast_records.ad_system
    , null                                                       as cpm_currency
    , null                                                       as cpm_price
    , tbl_missing_vast_records.ad_duration                       as duration
    , false                                                      as has_duration_mismatch
    , null                                                       as invalid_beacon_count
    , null                                                       as invalid_tracking_events
    , true                                                       as is_active
    , false                                                      as is_dup
    , null                                                       as is_empty
    , null                                                       as is_evergreen
    , null                                                       as is_fallback
    , false                                                      as is_fcapped
    , null                                                       as is_filled
    , false                                                      as is_fingerprint_dup
    , true                                                       as is_ingested
    , true                                                       as is_inserted
    , false                                                      as is_url_dup
    , null                                                       as is_philler
    , null                                                       as vast_depth
    , null                                                       as vast_latency_ms
    , null                                                       as vast_position
    , null                                                       as vast_status
    , null                                                       as vast_version
    , null                                                       as manifest_system
    , null                                                       as manifest_system_version
    , tbl_ad_pods.asset_type
    , tbl_ad_pods.client_type
    , tbl_ad_pods.network
    , tbl_ad_pods.channel
    , tbl_ad_pods.is_live_edge
    , tbl_ad_pods.owner
    , tbl_ad_pods.stitcher_status
    , false                                                      as has_vast_record
    , tbl_ad_pods.pod_instance_id
    , null                                                       as beacon_vast_difference_seconds
    , null                                                       as media_url
    , null                                                       as media_url_hash
    , null                                                       as has_url_mismatch
    , tbl_ad_pods.has_uid_token
    , tbl_ad_pods.has_aip_token
    , null                                                       as is_extra
    , null                                                       as is_expired
    , case when tbl_ad_pods.stitcher_status = 'unsaved_ad_pod' then 0
      else tbl_missing_vast_records.impression_count
    end                                                          as impression_count
    , case when tbl_ad_pods.stitcher_status = 'unsaved_ad_pod' then 0
      else tbl_missing_vast_records.complete_count
    end                                                          as complete_count
    , tbl_ad_pods.player_pod_id is not null                      as has_ad_pod_record
    , true                                                       as has_beacon_record
    , round(tbl_ad_pods.duration)::varchar || 's'                as pod_duration_seconds
  from {{ ref('tbl_missing_vast_records') }} as tbl_missing_vast_records
  left join {{ ref('tbl_ad_pods') }}
    on
      tbl_ad_pods.player_pod_id = tbl_missing_vast_records.player_pod_id
      and tbl_ad_pods.dedupe_number = 1
      and tbl_ad_pods.partition_date between (
        '{{ start_date }}'::date - interval '1 day'
      ) and (
        '{{ end_date }}'::date + interval '1 day'
      )
  where
    tbl_missing_vast_records.partition_date between
    '{{ start_date }}' and '{{ end_date }}'

)

, tbl_orphaned_ad_pods as (

  select
    tbl_orphaned_ad_pods.partition_date
    , tbl_orphaned_ad_pods.partition_date_hour
    , null                                                   as sent_at
    , tbl_orphaned_ad_pods.player_pod_id
    , split_part(tbl_orphaned_ad_pods.player_pod_id, ':', 2) as sutured_pid
    , split_part(tbl_orphaned_ad_pods.player_pod_id, ':', 1) as pod_id
    , tbl_orphaned_ad_pods.request_id
    , null                                                   as ad_ident
    , null                                                   as creative_id
    , null                                                   as dup_ident
    , null                                                   as provider_ident
    , null                                                   as ad_reseller
    , null                                                   as ad_system
    , null                                                   as cpm_currency
    , null                                                   as cpm_price
    , null                                                   as duration
    , null                                                   as has_duration_mismatch
    , null                                                   as invalid_beacon_count
    , null                                                   as invalid_tracking_events
    , null                                                   as is_active
    , null                                                   as is_dup
    , null                                                   as is_empty
    , null                                                   as is_evergreen
    , null                                                   as is_fallback
    , null                                                   as is_fcapped
    , null                                                   as is_filled
    , null                                                   as is_fingerprint_dup
    , null                                                   as is_ingested
    , null                                                   as is_inserted
    , null                                                   as is_url_dup
    , null                                                   as is_philler
    , null                                                   as vast_depth
    , null                                                   as vast_latency_ms
    , null                                                   as vast_position
    , null                                                   as vast_status
    , null                                                   as vast_version
    , null                                                   as manifest_system
    , null                                                   as manifest_system_version
    , tbl_orphaned_ad_pods.asset_type
    , tbl_orphaned_ad_pods.client_type
    , tbl_orphaned_ad_pods.network
    , tbl_orphaned_ad_pods.channel
    , tbl_orphaned_ad_pods.is_live_edge
    , tbl_orphaned_ad_pods.owner
    , tbl_orphaned_ad_pods.stitcher_status
    , false                                                  as has_vast_record
    , tbl_orphaned_ad_pods.pod_instance_id
    , null                                                   as beacon_vast_difference_seconds
    , null                                                   as media_url
    , null                                                   as media_url_hash
    , null                                                   as has_url_mismatch
    , tbl_orphaned_ad_pods.has_uid_token
    , tbl_orphaned_ad_pods.has_aip_token
    , null                                                   as is_extra
    , null                                                   as is_expired
    , null                                                   as impression_count
    , null                                                   as complete_count
    , false                                                  as has_beacon_record
    , true                                                   as has_ad_pod_record
    , round(tbl_orphaned_ad_pods.duration)::varchar || 's'   as pod_duration_seconds
  from {{ ref('tbl_orphaned_ad_pods') }} as tbl_orphaned_ad_pods

)

, all_records as (
  select * from tbl_vast_ads_beacons
  union all
  select * from tbl_missing_vast_records
  union all
  select * from tbl_orphaned_ad_pods
)

select
  all_records.partition_date
  , all_records.partition_date_hour
  , all_records.sent_at
  , all_records.player_pod_id
  , all_records.request_id
  , all_records.sutured_pid
  , all_records.pod_id
  , all_records.ad_ident
  , all_records.creative_id
  , all_records.dup_ident
  , all_records.provider_ident
  , all_records.ad_reseller
  , all_records.ad_system
  , all_records.cpm_currency
  , all_records.duration
  , all_records.has_duration_mismatch
  , all_records.invalid_beacon_count
  , all_records.invalid_tracking_events
  , all_records.is_active
  , all_records.is_dup
  , all_records.is_empty
  , all_records.is_evergreen
  , all_records.is_fallback
  , all_records.is_fcapped
  , all_records.is_filled
  , all_records.is_fingerprint_dup
  , all_records.is_ingested
  , all_records.is_inserted
  , all_records.is_url_dup
  , all_records.is_philler
  , all_records.vast_depth
  , all_records.vast_latency_ms
  , all_records.vast_position
  , all_records.vast_status
  , all_records.vast_version
  , all_records.manifest_system
  , all_records.manifest_system_version
  , all_records.impression_count
  , all_records.complete_count
  , tbl_playback_session_attributes.device_name
  , tbl_playback_session_attributes.dma
  , tbl_playback_session_attributes.os_version
  , tbl_playback_session_attributes.app_version
  , all_records.asset_type
  , all_records.client_type
  , all_records.network
  , all_records.channel
  , all_records.is_live_edge
  , all_records.owner
  , all_records.stitcher_status
  , all_records.has_ad_pod_record
  , all_records.has_vast_record
  , all_records.has_beacon_record
  , tbl_join_user_id__sutured_pid.user_id
  , all_records.pod_duration_seconds
  , all_records.pod_instance_id
  , all_records.cpm_price
  , all_records.beacon_vast_difference_seconds
  , all_records.media_url
  , all_records.media_url_hash
  , all_records.has_url_mismatch
  , all_records.has_uid_token
  , all_records.has_aip_token
  , all_records.is_extra
  , all_records.is_expired
  , case when all_records.impression_count >= 1 then 1 else 0 end as unique_impression_count
  , case when all_records.complete_count >= 1 then 1 else 0 end   as unique_complete_count
from all_records
left join {{ ref('tbl_playback_session_attributes') }} as tbl_playback_session_attributes
  on all_records.sutured_pid = tbl_playback_session_attributes.sutured_pid
left join {{ ref('tbl_join_user_id__sutured_pid') }} as tbl_join_user_id__sutured_pid
  on all_records.sutured_pid = tbl_join_user_id__sutured_pid.sutured_pid
    and tbl_join_user_id__sutured_pid.partition_date
    between '{{ start_date }}'::date - interval '10 day'
    and '{{ end_date }}'::date + interval '10 day'
