{{ config(materialized='view'
, tags=["dai", "exclude_hourly", "exclude_daily"]
) }}


--as columns are added to the underlying table, the ordering should be updated to maintain a logical grouping

select
  partition_date
  , partition_date_hour
  , sent_at
  , user_id
  , {{ pod_instance_id(source = 'tbl_vast_ads_enriched', player_id = 'sutured_pid') }}       as pod_instance_id
  , client_type
  , has_vast_record
  , has_ad_pod_record
  , has_beacon_record
  , {{ cpm_price(source = 'tbl_vast_ads_enriched') }}                                        as cpm_price
  , {{ is_guaranteed_unpaid(source = 'tbl_vast_ads_enriched') }}                             as is_guaranteed_unpaid
  , {{ is_viable('tbl_vast_ads_enriched') }}                                                 as is_viable
  , impression_count
  , complete_count
  , network
  , channel
  , owner
  , device_name
  , duration
  , invalid_beacon_count
  , invalid_tracking_events
  , is_philler
  , is_active
  , is_dup
  , is_empty
  , is_evergreen
  , is_fallback
  , is_fcapped
  , is_filled
  , is_fingerprint_dup
  , is_ingested
  , is_inserted
  , is_url_dup
  , vast_depth
  , vast_latency_ms
  , vast_position
  , manifest_system_version
  , is_live_edge
  , player_pod_id
  , request_id
  , sutured_pid
  , pod_id
  , ad_ident
  , creative_id
  , dup_ident
  , provider_ident
  , ad_reseller
  , ad_system
  , cpm_currency
  , vast_status
  , vast_version
  , manifest_system
  , dma
  , os_version
  , app_version
  , asset_type
  , has_duration_mismatch
  , stitcher_status
  , beacon_vast_difference_seconds
  , pod_duration_seconds
  , unique_impression_count
  , unique_complete_count
  , media_url
  , media_url_hash
  , has_url_mismatch
  , has_uid_token
  , has_aip_token
  , is_extra
  , is_expired
from {{ ref('tbl_vast_ads_enriched') }}
