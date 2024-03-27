{{ config(
materialized='tuple_incremental'
, sort=['partition_date', 'player_pod_id']
, dist='player_pod_id'
, tags=["exclude_hourly", "exclude_daily", "dai"]
, enabled=false
, unique_key = ['partition_date']
, on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with ad_pods as (
  select *
  from {{ ref('tbl_ad_pods') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, beacons as (
  select *
  from {{ ref('tbl_ad_beacons_summary') }}
  where partition_date between '{{ start_date }}' and '{{ end_date }}'
)

select
  ad_pods.partition_date
  , ad_pods.player_pod_id
  , ad_pods.partition_date_hour
  , ad_pods.sutured_pid
  , ad_pods.event_timestamp
  , ad_pods.request_id
  , ad_pods.asset_type
  , ad_pods.channel
  , ad_pods.network
  , ad_pods.client_type
  , ad_pods.duplicate_ads
  , ad_pods.duration
  , ad_pods.duration_mismatch_ads
  , ad_pods.fill_duration
  , ad_pods.fallback_ads
  , ad_pods.frequency_capped_ads
  , ad_pods.house_ads
  , ad_pods.house_duration
  , ad_pods.inactive_ads
  , ad_pods.ingested_ads
  , ad_pods.inserted_ads
  , ad_pods.inserted_house_ads_duration
  , ad_pods.inserted_house_ads
  , ad_pods.inserted_paid_ads_duration
  , ad_pods.inserted_paid_ads
  , ad_pods.inserted_philler_ads_duration
  , ad_pods.inserted_philler_ads
  , ad_pods.is_live_edge
  , ad_pods.owner
  , ad_pods.pod_id
  , ad_pods.primary_ads
  , ad_pods.received_ads
  , ad_pods.status
  , ad_pods.stitcher_status
  , ad_pods.track
  , ad_pods.uningested_ads
  , ad_pods.manifest_system
  , ad_pods.manifest_system_version
  , ad_pods.dedupe_number
  , case when beacons.player_pod_id is not null then 1 else 0 end as has_beacon_records
  , coalesce(beacons.distributor_house_count_impression, 0)       as distributor_house_count_impression
  , coalesce(beacons.distributor_house_count_complete, 0)         as distributor_house_count_complete
  , coalesce(beacons.distributor_house_duration_impression, 0)    as distributor_house_duration_impression
  , coalesce(beacons.distributor_house_duration_complete, 0)      as distributor_house_duration_complete
  , coalesce(beacons.distributor_nhouse_count_impression, 0)      as distributor_nhouse_count_impression
  , coalesce(beacons.distributor_nhouse_count_complete, 0)        as distributor_nhouse_count_complete
  , coalesce(beacons.distributor_nhouse_duration_impression, 0)   as distributor_nhouse_duration_impression
  , coalesce(beacons.distributor_nhouse_duration_complete, 0)     as distributor_nhouse_duration_complete
  , coalesce(beacons.distributor_total_count_impression, 0)       as distributor_total_count_impression
  , coalesce(beacons.distributor_total_count_complete, 0)         as distributor_total_count_complete
  , coalesce(beacons.distributor_total_duration_impression, 0)    as distributor_total_duration_impression
  , coalesce(beacons.distributor_total_duration_complete, 0)      as distributor_total_duration_complete
  , coalesce(beacons.provider_house_count_impression, 0)          as provider_house_count_impression
  , coalesce(beacons.provider_house_count_complete, 0)            as provider_house_count_complete
  , coalesce(beacons.provider_house_duration_impression, 0)       as provider_house_duration_impression
  , coalesce(beacons.provider_house_duration_complete, 0)         as provider_house_duration_complete
  , coalesce(beacons.provider_nhouse_count_impression, 0)         as provider_nhouse_count_impression
  , coalesce(beacons.provider_nhouse_count_complete, 0)           as provider_nhouse_count_complete
  , coalesce(beacons.provider_nhouse_duration_impression, 0)      as provider_nhouse_duration_impression
  , coalesce(beacons.provider_nhouse_duration_complete, 0)        as provider_nhouse_duration_complete
  , coalesce(beacons.provider_total_count_impression, 0)          as provider_total_count_impression
  , coalesce(beacons.provider_total_count_complete, 0)            as provider_total_count_complete
  , coalesce(beacons.provider_total_duration_impression, 0)       as provider_total_duration_impression
  , coalesce(beacons.provider_total_duration_complete, 0)         as provider_total_duration_complete
from ad_pods
left outer join beacons
  on ad_pods.partition_date = beacons.partition_date
    and ad_pods.player_pod_id = beacons.player_pod_id
    and ad_pods.dedupe_number = 1
