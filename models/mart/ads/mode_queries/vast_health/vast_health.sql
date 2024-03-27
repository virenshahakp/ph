{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date', 'asset_type_derived']
  , sort=[
    'partition_date'
    , 'sutured_pid__pod_id'
    , 'client_type'
    , 'asset_type_derived'
    , 'network'
    , 'channel'
  ]
  , dist='sutured_pid__pod_id'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }} --TODO: DEV-15587 because the tbl uses the sutured_pid__pod_id as its dist and sort key, the field will need to be maintained when pod_instance_id is added

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}


with vast_ads as (

  select                                                                                                    --noqa: L034
    fct_vast_ads.sutured_pid
    , fct_vast_ads.pod_id
    , fct_vast_ads.sutured_pid || ':' || fct_vast_ads.pod_id as sutured_pid__pod_id --TODO: DEV-15587 add standardize pod_instance_id via its macro
    , fct_vast_ads.is_empty
    , fct_vast_ads.is_filled
    , fct_vast_ads.is_inserted
    , fct_vast_ads.is_fallback
    , fct_vast_ads.is_ingested
    , fct_vast_ads.is_dup
    , fct_vast_ads.is_url_dup
    , fct_vast_ads.is_active
    , fct_vast_ads.is_evergreen
    /*
    is_evergreen_by_price is derived and needs more verification.
    the original version had fct_vast_ads.cpm."price" = '1' which is an exception for a&e promos
    , (fct_vast_ads.cpm."price" is null or fct_vast_ads.cpm."price" = '1')
    , (fct_vast_ads.cpm."price" is null or to_number(fct_vast_ads.cpm."price", '999d9') <= 1)
    */
    , (
      fct_vast_ads.is_evergreen
      or fct_vast_ads.cpm_price = '0'
    )                                                        as is_evergreen_by_price
    , fct_vast_ads.is_fingerprint_dup
    , fct_vast_ads.is_fcapped
    , fct_vast_ads.has_duration_mismatch
    , fct_vast_ads.duration
    , fct_vast_ads.invalid_tracking_events
    , fct_vast_ads.invalid_beacon_count
    , fct_vast_ads.cpm_price
    , fct_vast_ads.is_viable
  from {{ ref('fct_vast_ads_enriched') }} as fct_vast_ads
  where fct_vast_ads.partition_date between '{{ start_date }}' and '{{ end_date }}'
    and fct_vast_ads.manifest_system_version >= 1
)


, ad_pods as (
  select                                                                                                    --noqa: L034
    fct_ad_pods.sutured_pid
    , fct_ad_pods.pod_id
    , fct_ad_pods.sutured_pid || ':' || fct_ad_pods.pod_id as sutured_pid__pod_id --TODO: DEV-15587 add standardize pod_instance_id via its macro
    , fct_ad_pods.asset_type
    , fct_ad_pods.is_live_edge
    , case
      when
        fct_ad_pods.asset_type = 'live'
        and fct_ad_pods.is_live_edge
        then 'live-edge'
      when
        fct_ad_pods.asset_type = 'live'
        and not fct_ad_pods.is_live_edge
        then 'live-non-edge'
      else fct_ad_pods.asset_type
    end                                                    as asset_type_derived
    , fct_ad_pods.network
    , fct_ad_pods.channel
    , fct_ad_pods.client_type
    , fct_ad_pods.stitcher_status
    , fct_ad_pods.duration
    , fct_ad_pods.partition_date
    , fct_ad_pods.event_timestamp
    , row_number() over (
      partition by
        fct_ad_pods.sutured_pid
        , fct_ad_pods.pod_id
    )                                                      as ad_pod_number
    , listagg(
      fct_ad_pods.stitcher_status
      , ','
    ) within group (
      order by fct_ad_pods.stitcher_status
    ) over (
      partition by
        fct_ad_pods.sutured_pid
        , fct_ad_pods.pod_id
    )                                                      as stitcher_statuses
    , fct_ad_pods.inserted_house_ads_duration
    , fct_ad_pods.inserted_paid_ads_duration
  from {{ ref('tbl_ad_pods') }} as fct_ad_pods
  where fct_ad_pods.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, vast_ads_agumented_with_ad_pod as (
  select
    ad_pods.network
    , ad_pods.channel
    , ad_pods.client_type
    , ad_pods.stitcher_status
    , ad_pods.duration  as pod_duration
    , ad_pods.inserted_house_ads_duration
    , ad_pods.inserted_paid_ads_duration
    , ad_pods.partition_date
    , ad_pods.event_timestamp
    , ad_pods.asset_type
    , ad_pods.is_live_edge
    , ad_pods.asset_type_derived
    , vast_ads.sutured_pid
    , vast_ads.pod_id
    , vast_ads.sutured_pid__pod_id
    , vast_ads.is_empty
    , vast_ads.is_filled
    , vast_ads.is_inserted
    , vast_ads.is_fallback
    , vast_ads.is_ingested
    , vast_ads.is_dup
    , vast_ads.is_url_dup
    , vast_ads.is_active
    , vast_ads.is_evergreen
    , vast_ads.is_evergreen_by_price
    , vast_ads.is_fingerprint_dup
    , vast_ads.is_fcapped
    , vast_ads.has_duration_mismatch
    , vast_ads.duration as ad_duration
    , vast_ads.invalid_tracking_events
    , vast_ads.invalid_beacon_count
    , vast_ads.cpm_price
    , vast_ads.is_viable
  from vast_ads
  join ad_pods
    on vast_ads.sutured_pid = ad_pods.sutured_pid
      and vast_ads.pod_id = ad_pods.pod_id
  where ad_pods.ad_pod_number = 1
    -- https://philoinc.slack.com/archives/c03tehur136/p1661808065679249?thread_ts=1661805621.684579&cid=c03tehur136        --noqa: L016
    -- filter out any ad pods that where called twice becuase they will introduce double counting when joinign to vast ads  --noqa: L016
    and ad_pods.stitcher_statuses not ilike '%unsaved%'
)

, pods_augmented_with_vast_ads as (
  select
    event_timestamp
    , partition_date
    , network
    , channel
    , client_type
    , stitcher_status
    , pod_duration
    , sutured_pid
    , pod_id
    , sutured_pid__pod_id
    , asset_type
    , is_live_edge
    , asset_type_derived
    , is_empty
    , inserted_house_ads_duration
    , inserted_paid_ads_duration

    , sum(
      case
        when
          is_evergreen is true
          --is_evergreen_by_price is true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_house_received

    , sum(
      case
        when
          is_evergreen is true
          --is_evergreen_by_price is true
          and is_viable is true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_house_viable

    , sum(
      case
        when
          is_evergreen is true
          --is_evergreen_by_price is true
          and is_viable is not true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_house_nonviable

    , sum(
      case
        when
          is_evergreen is not true
          --is_evergreen_by_price is not true
          and is_fallback is not true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_paid_primary_received

    , sum(
      case
        when
          is_evergreen is not true
          --is_evergreen_by_price is not true
          and is_fallback is not true
          and is_viable is true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_paid_primary_viable

    , sum(
      case
        when
          is_evergreen is not true
          --is_evergreen_by_price is not true
          and is_fallback is not true
          and is_viable is not true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_paid_primary_nonviable

    , sum(
      case
        when
          is_evergreen is not true
          --is_evergreen_by_price is not true
          and is_fallback is true
          and is_viable is true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_paid_fallback_received

    , sum(
      case
        when
          is_evergreen is not true
          --is_evergreen_by_price is not true
          and is_fallback is true
          and is_viable is true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_paid_fallback_viable

    , sum(
      case
        when
          is_evergreen is not true
          --is_evergreen_by_price is not true
          and is_fallback is true
          and is_viable is not true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_paid_fallback_nonviable

    , sum(ad_duration)                               as seconds_ads_received

    , sum(
      case
        when
          is_viable is true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_viable

    , sum(
      case
        when
          is_viable is not true
          then ad_duration
        else 0
      end
    )                                                as seconds_ads_nonviable

    , sum(cpm_price::float) / 1000                   as revenue

    , pod_duration - seconds_ads_paid_primary_viable as seconds_paid_deficit
    , pod_duration - (
      seconds_ads_paid_primary_viable
      + seconds_ads_paid_fallback_viable
    )                                                as seconds_paid_and_paidfallback_deficit
    , pod_duration - (
      seconds_ads_paid_primary_viable
      + seconds_ads_paid_fallback_viable
      + seconds_ads_house_viable
    )                                                as seconds_all_deficit
  from vast_ads_agumented_with_ad_pod
  {{ dbt_utils.group_by(n=16) }}
)

select
  event_timestamp
  , partition_date
  , network
  , channel
  , client_type
  , stitcher_status
  , pod_duration
  , sutured_pid
  , pod_id
  , sutured_pid__pod_id
  , asset_type
  , is_live_edge
  , asset_type_derived
  , is_empty
  , inserted_house_ads_duration
  , inserted_paid_ads_duration

  , seconds_ads_house_received
  , seconds_ads_house_viable
  , seconds_ads_house_nonviable
  , seconds_ads_paid_primary_received
  , seconds_ads_paid_primary_viable
  , seconds_ads_paid_primary_nonviable
  , seconds_ads_paid_fallback_received
  , seconds_ads_paid_fallback_viable
  , seconds_ads_paid_fallback_nonviable

  , seconds_ads_received
  , seconds_ads_viable
  , seconds_ads_nonviable

  , revenue
  , seconds_paid_deficit
  , seconds_paid_and_paidfallback_deficit
  , seconds_all_deficit

  , round(
    seconds_ads_received::float
    / pod_duration
    , 2
  ) as received_ads_p
  , round(
    seconds_ads_viable::float
    / pod_duration
    , 2
  ) as viable_ads_p
  , round(
    seconds_ads_nonviable::float
    / pod_duration
    , 2
  ) as nonviable_ads_p
  , round(
    seconds_ads_house_received::float
    / pod_duration
    , 2
  ) as received_house_ads_p
  , round(
    seconds_ads_house_viable::float
    / pod_duration::float
    , 2
  ) as viable_house_ads_p
  , round(
    seconds_ads_house_nonviable::float
    / pod_duration::float
    , 2
  ) as nonviable_house_ads_p
  , round(
    seconds_ads_paid_primary_received::float
    / pod_duration::float
    , 2
  ) as received_paid_ads_primary_p
  , round(
    (
      seconds_ads_paid_primary_received::float
      + seconds_ads_paid_fallback_received::float
    )
    / pod_duration::float
    , 2
  ) as received_paid_ads_p

  , round(
    seconds_ads_paid_primary_viable::float
    / pod_duration::float
    , 2
  ) as viable_paid_ads_primary_p
  , round(
    (
      seconds_ads_paid_primary_viable::float
      + seconds_ads_paid_fallback_viable::float
    )
    / pod_duration::float
    , 2
  ) as viable_paid_ads_p
  , round(
    seconds_ads_paid_primary_nonviable::float
    / pod_duration::float
    , 2
  ) as nonviable_paid_ads_primary_p
  , round(
    (
      seconds_ads_paid_primary_nonviable::float
      + seconds_ads_paid_fallback_nonviable::float
    )
    / pod_duration::float
    , 2
  ) as nonviable_paid_ads_p

  , round(
    inserted_paid_ads_duration::float
    / pod_duration::float
    , 2
  ) as inserted_paid_ads_p
from pods_augmented_with_vast_ads
