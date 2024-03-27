{{ config(
    materialized='tuple_incremental'
    , sort=['partition_date','hash_fingerprint']
    , dist='hash_fingerprint'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , unique_key = ['partition_date']
    , on_schema_change = 'append_new_columns'
    , enabled=false
) }} --TODO: DEV-15905 candidate for removal. model was a debugging tool, no need to have it comitted to the repo

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

select
  partition_date
  , sent_at
  , partition_hour
  , partition_date_hour
  , player_pod_id
  , pod_id
  , request_id
  , ad_ident
  , creative_id
  , dup_ident
  , provider_ident
  , ad_reseller
  , ad_system
  , cpm_currency
  , duration
  , has_duration_mismatch
  , invalid_beacon_count
  , invalid_tracking_events
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
  , is_wrapper
  , vast_depth
  , vast_latency_ms
  , vast_position
  , vast_status
  , vast_version
  , manifest_system
  , manifest_system_version
  , hash_fingerprint
  , sutured_pid
  , is_philler
  , pod_instance_id
  , media_url
  , media_url_hash
  , has_url_mismatch
  , {{ cpm_price(source = "tbl_vast_ads") }}                                    as cpm_price -- noqa: LT01
  , {{ is_guaranteed_unpaid(source = "tbl_vast_ads") }}                         as is_guaranteed_unpaid -- noqa: LT01
  , {{ is_viable(source = "tbl_vast_ads") }}                                    as is_viable -- noqa: LT01
from {{ ref('tbl_vast_ads') }}
where partition_date between '{{ start_date }}' and '{{ end_date }}'