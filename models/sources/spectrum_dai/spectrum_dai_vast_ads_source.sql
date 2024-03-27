select
  --time
  year                                         as year                  --noqa: L029
  , month                                      as month                 --noqa: L029
  , day                                        as day                   --noqa: L029
  , (year || '-' || month || '-' || day)::date as partition_date
  , hour                                       as hour                  --noqa: L029
  , created_at                                 as created_at
  , sent_at                                    as sent_at
  --pod instance ids
  , player_id                                  as sutured_pid
  , pod_id                                     as pod_id
  , request_id                                 as request_id
  --ad ids
  , ad_ident                                   as ad_ident
  , creative_id                                as creative_id
  , dup_ident                                  as dup_ident
  , audio_dup_ident                            as audio_dup_ident
  --bidder ids
  , provider_ident                             as provider_ident
  , ad_reseller                                as ad_reseller
  , ad_system                                  as ad_system
  --dimensional information
  , ad_position                                as ad_position
  , cpm.currency                               as cpm_currency
  , cpm.price                                  as cpm_price
  , duration                                   as duration
  , has_duration_mismatch                      as has_duration_mismatch
  , invalid_beacon_count                       as invalid_beacon_count
  , invalid_tracking_events                    as invalid_tracking_events
  , is_active                                  as is_active
  , is_audio_dup                               as is_audio_dup
  , is_dup                                     as is_dup
  , is_empty                                   as is_empty
  , is_evergreen                               as is_evergreen
  , is_fallback                                as is_fallback
  , is_fcapped                                 as is_fcapped
  , is_filled                                  as is_filled
  , is_fingerprint_dup                         as is_fingerprint_dup
  , is_house                                   as is_house
  , is_ingested                                as is_ingested
  , is_inserted                                as is_inserted
  , is_url_dup                                 as is_url_dup
  , is_wrapper                                 as is_wrapper
  , vast_depth                                 as vast_depth
  , vast_latency_ms                            as vast_latency_ms
  , vast_position                              as vast_position
  , vast_status                                as vast_status
  , vast_version                               as vast_version
  , version                                    as version               --noqa: L029

from {{ source('spectrum_dai', 'vast_ads') }}

{%- if target.name != 'prod' %}
  where partition_date::date >= ({{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }})
{%- endif -%}
