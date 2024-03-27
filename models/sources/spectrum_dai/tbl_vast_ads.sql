{{ config(
    materialized='tuple_incremental'
    , sort=['partition_date','hash_fingerprint']
    , dist='hash_fingerprint'
    , tags=["exclude_hourly", "exclude_daily", "dai"]
    , unique_key = ['partition_date']
    , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

select
  (year || '-' || month || '-' || day)::date                         as partition_date
  , hour                                                             as partition_hour
  , player_id                                                        as sutured_pid
  , pod_id                                                           as pod_id
  , request_id                                                       as request_id
  , ad_ident                                                         as ad_ident
  , duration                                                         as duration
  , has_duration_mismatch                                            as has_duration_mismatch
  , invalid_beacon_count                                             as invalid_beacon_count
  , invalid_tracking_events                                          as invalid_tracking_events
  , is_active                                                        as is_active
  , is_dup                                                           as is_dup
  , is_empty                                                         as is_empty
  , is_fallback                                                      as is_fallback
  , is_fcapped                                                       as is_fcapped
  , is_filled                                                        as is_filled
  , is_ingested                                                      as is_ingested
  , is_inserted                                                      as is_inserted
  , is_url_dup                                                       as is_url_dup
  , is_wrapper                                                       as is_wrapper
  , vast_depth                                                       as vast_depth
  , vast_latency_ms                                                  as vast_latency_ms
  , vast_position                                                    as vast_position
  , vast_status                                                      as vast_status
  , is_extra
  , is_expired
  , media_url
  , media_url_hash
  , has_url_mismatch
  , version                                                          as manifest_system_version
  , cpm.currency                                                     as cpm_currency
  , fnv_hash(
    partition_date
    , fnv_hash(
      partition_hour
      , fnv_hash(
        player_id
        , fnv_hash(
          pod_id
          , fnv_hash(
            coalesce(dup_ident, audio_dup_ident)
            , fnv_hash(
              coalesce(provider_ident, ad_system || ':' || creative_id)
              , fnv_hash(
                vast_depth
                , fnv_hash(
                  vast_position
                )
              )
            )
          )
        )
      )
    )
  )                                                                  as hash_fingerprint
  , coalesce(sent_at, created_at)                                    as sent_at
  , pod_id || ':' || player_id                                       as player_pod_id
  , {{ pod_instance_id(source = 'vast_ads') }}                as pod_instance_id
  , coalesce(creative_id, split_part(provider_ident, ':', 2))        as creative_id
  , coalesce(dup_ident, audio_dup_ident)                             as dup_ident
  , coalesce(provider_ident, ad_system || ':' || creative_id)        as provider_ident
  , nullif(ad_reseller, '')                                          as ad_reseller
  , coalesce(ad_system, split_part(provider_ident, ':', 1))          as ad_system
  , cpm.price::float                                                 as cpm_price
  , coalesce(is_evergreen, is_house)                                 as is_evergreen
  , coalesce(is_fingerprint_dup, is_audio_dup)                       as is_fingerprint_dup
  , coalesce(is_philler, false)                                      as is_philler
  , nullif(vast_version, '')                                         as vast_version
  --version information started being passed with stitcher.  This value starts at 1 and increases
  , case
    when version is null
      then 'sutured'
    else 'stitcher'
  end                                                                as manifest_system
  , date_add('hour', partition_hour::int, partition_date::timestamp) as partition_date_hour
from {{ source('spectrum_dai', 'vast_ads') }}
where partition_date between '{{ start_date }}' and '{{ end_date }}'