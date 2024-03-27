{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}
{% set end_date_alias = dates.end_date_alias %}

{{ config(
  materialized='table'
  , alias='tbl_vast_ads_beacons_' + end_date_alias
  , sort=['partition_date', 'partition_date_hour', 'player_pod_id' ]
  , dist='player_pod_id'
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}

select
  tbl_vast_ads.partition_date
  , tbl_vast_ads.partition_date_hour
  , tbl_vast_ads.sent_at
  , tbl_vast_ads.player_pod_id
  , tbl_vast_ads.sutured_pid
  , tbl_vast_ads.pod_id
  , tbl_vast_ads.request_id
  , tbl_vast_ads.ad_ident
  , tbl_vast_ads.creative_id
  , tbl_vast_ads.dup_ident
  , tbl_vast_ads.provider_ident
  , tbl_vast_ads.ad_reseller
  , tbl_vast_ads.ad_system
  , tbl_vast_ads.cpm_currency
  , tbl_vast_ads.cpm_price
  , tbl_vast_ads.duration
  , tbl_vast_ads.has_duration_mismatch
  , tbl_vast_ads.invalid_beacon_count
  , tbl_vast_ads.invalid_tracking_events
  , tbl_vast_ads.is_active
  , tbl_vast_ads.is_dup
  , tbl_vast_ads.is_empty
  , tbl_vast_ads.is_evergreen
  , tbl_vast_ads.is_fallback
  , tbl_vast_ads.is_fcapped
  , tbl_vast_ads.is_filled
  , tbl_vast_ads.is_fingerprint_dup
  , tbl_vast_ads.is_ingested
  , tbl_vast_ads.is_inserted
  , tbl_vast_ads.is_url_dup
  , tbl_vast_ads.is_philler
  , tbl_vast_ads.vast_depth
  , tbl_vast_ads.vast_latency_ms
  , tbl_vast_ads.vast_position
  , tbl_vast_ads.vast_status
  , tbl_vast_ads.vast_version
  , tbl_vast_ads.manifest_system
  , tbl_vast_ads.manifest_system_version
  , tbl_vast_ads.hash_fingerprint
  , tbl_ad_beacons_flat.first_beacon_logged_at
  , tbl_vast_ads.pod_instance_id --TODO: DEV-15587 remove from this model and carry the constituent parts downstream
  , tbl_vast_ads.media_url
  , tbl_vast_ads.media_url_hash
  , tbl_vast_ads.has_url_mismatch
  , tbl_vast_ads.is_extra
  , tbl_vast_ads.is_expired
  , coalesce(tbl_ad_beacons_flat.impression_count, 0)                                as impression_count
  , coalesce(tbl_ad_beacons_flat.complete_count, 0)                                  as complete_count
  , date_diff('s', tbl_vast_ads.sent_at, tbl_ad_beacons_flat.first_beacon_logged_at) as beacon_vast_difference_seconds
from {{ ref('tbl_vast_ads') }}
left join {{ ref('tbl_ad_beacons_flat') }} as tbl_ad_beacons_flat
  on tbl_vast_ads.player_pod_id = tbl_ad_beacons_flat.player_pod_id
    and tbl_vast_ads.creative_id = tbl_ad_beacons_flat.creative_id
    and tbl_vast_ads.ad_system = tbl_ad_beacons_flat.ad_system
    and tbl_vast_ads.is_inserted is true
    and tbl_ad_beacons_flat.partition_date between ('{{ start_date }}'::date - interval '1 day')
      and ('{{ end_date }}'::date + interval '1 day') --noqa: L003 
where tbl_vast_ads.partition_date between '{{ start_date }}' and '{{ end_date }}'
  and tbl_vast_ads.is_wrapper is false


