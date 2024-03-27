{{ config(
  materialized='tuple_incremental'
  , unique_key=['partition_date']
  , sort=[
    'partition_date'
    , 'platform'
    , 'asset_type'
    , 'network'
    , 'channel'
    , 'ad_server'
    , 'vmap_uuid'
    , 'endpoint_uuid'
  ]
  , dist='endpoint_uuid'
  , full_refresh = false
  , tags=["dai", "exclude_hourly", "exclude_daily"]
  , on_schema_change = 'append_new_columns'
) }}

{% set dates = get_update_dates(2) %}
{% set start_date = dates.start_date %}
{% set end_date = dates.end_date %}

with bid_error as (
  select * from {{ ref('publica_bid_error_dyn_source') }}
  where publica_bid_error_dyn_source.partition_date between '{{ start_date }}' and '{{ end_date }}'
)

, demand_partner_map as (
  select * from {{ ref('dim_demand_partner_map') }}
)

, publica_platform_map as (
  select * from {{ ref('dim_publica_platform_map') }}
)


select
  bid_error.partition_date
  , bid_error.partition_hour
  , publica_platform_map.client_type                  as platform
  , demand_partner_map.is_paid                        as is_paid
  , demand_partner_map.is_count                       as is_count
  , bid_error.requested_pod_duration                  as requested_pod_duration
  , bid_error.bidrequest_device_geo_country           as geo_device_country
  , bid_error.bidrequest_device_geo_region            as geo_device_region
  , bid_error.bidrequest_device_geo_metro             as geo_device_metro
  , bid_error.bidrequest_device_geo_city              as geo_device_city
  , bid_error.bidrequest_device_geo_zip               as geo_device_zip
  , bid_error.vmap_uuid                               as vmap_uuid
  , bid_error.endpoint_uuid                           as endpoint_uuid
  , 'publica'::varchar                                as ad_server
  , lower(bid_error.content_network)                  as network
  , lower(bid_error.content_channel)                  as channel
  , case
    when bid_error.livestream = 1 then 'live'
    when bid_error.livestream = 2 then 'vod'
    when bid_error.livestream = 3 then 'dvr'
  end                                                 as asset_type
  , count(1)                                          as error_count
  , count(distinct demand_partner_map.demand_partner) as demand_partner_count
  , count(distinct demand_partner_map.bidder_name)    as bidder_count
  , count(distinct bid_error.bidder_tier)             as bidder_tier_count
  , count(distinct bid_error.custom_adomain)          as adomain_count
  , count(distinct bid_error.bidresponse_creative_id) as creative_id_count
  , count(distinct bid_error.bidresponse_cpm)         as distinct_cpm_count
  , min(bid_error.bidresponse_cpm)                    as min_cpm
  , max(bid_error.bidresponse_cpm)                    as max_cpmdb
  , sum(coalesce(bid_error.bidresponse_cpm, 0))       as cpm_sum
  , count(distinct bid_error.error_code)              as distinct_error_code_count
  , sum(
    case
      when bid_error.recognition_is_missing_adomain = true
        then 1
      else 0
    end
  )                                                   as recognition_is_missing_adomain_count
  , sum(
    case
      when bid_error.recognition_is_missing_category = true
        then 1
      else 0
    end
  )                                                   as recognition_is_missing_category_count

  -- pivoted error code events
  , sum(
    case
      when bid_error.error_code = 0
        then 1
      else 0
    end
  )                                                   as undefined__error_count
  , sum(
    case
      when bid_error.error_code = 1
        then 1
      else 0
    end
  )                                                   as timeout_error__error_count
  , sum(
    case
      when bid_error.error_code = 2
        then 1
      else 0
    end
  )                                                   as bad_input_error__error_count
  , sum(
    case
      when bid_error.error_code = 3
        then 1
      else 0
    end
  )                                                   as bad_server_response_error__error_count
  , sum(
    case
      when bid_error.error_code = 4
        then 1
      else 0
    end
  )                                                   as failed_to_request_bids_error__error_count
  , sum(
    case
      when bid_error.error_code = 5
        then 1
      else 0
    end
  )                                                   as bid_auction_failed__error_count
  , sum(
    case
      when bid_error.error_code = 6
        then 1
      else 0
    end
  )                                                   as rate_limit_blocked__error_count
  , sum(
    case
      when bid_error.error_code = 7
        then 1
      else 0
    end
  )                                                   as failed_to_parse_response__error_count
  , sum(
    case
      when bid_error.error_code = 8
        then 1
      else 0
    end
  )                                                   as connection_error__error_count
  , sum(
    case
      when bid_error.error_code = 9
        then 1
      else 0
    end
  )                                                   as panic__error_count
  , sum(
    case
      when bid_error.error_code = 10
        then 1
      else 0
    end
  )                                                   as pre_bid_error__error_count
  , sum(
    case
      when bid_error.error_code = 11
        then 1
      else 0
    end
  )                                                   as empty_response_error__error_count
  , sum(
    case
      when bid_error.error_code = 12
        then 1
      else 0
    end
  )                                                   as no_content_204_error__error_count
  , sum(
    case
      when bid_error.error_code = 13
        then 1
      else 0
    end
  )                                                   as no_fill_error__error_count
  , sum(
    case
      when bid_error.error_code = 14
        then 1
      else 0
    end
  )                                                   as empty_vast_error__error_count
  , sum(
    case
      when bid_error.error_code = 15
        then 1
      else 0
    end
  )                                                   as vast_validate_error__error_count
  , sum(
    case
      when bid_error.error_code = 16
        then 1
      else 0
    end
  )                                                   as cache_put_cache_miss__error_count
  , sum(
    case
      when bid_error.error_code = 17
        then 1
      else 0
    end
  )                                                   as unwrap_timeout_error__error_count
  , sum(
    case
      when bid_error.error_code = 18
        then 1
      else 0
    end
  )                                                   as bid_price_below_floor__error_count
  , sum(
    case
      when bid_error.error_code = 19
        then 1
      else 0
    end
  )                                                   as unknown_error__error_count
  , sum(
    case
      when bid_error.error_code = 20
        then 1
      else 0
    end
  )                                                   as bid_rejected_unknown__error_count
  , sum(
    case
      when bid_error.error_code = 21
        then 1
      else 0
    end
  )                                                   as bid_rejected_duration__error_count
  , sum(
    case
      when bid_error.error_code = 22
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_media_file_url__error_count
  , sum(
    case
      when bid_error.error_code = 23
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_adomain__error_count
  , sum(
    case
      when bid_error.error_code = 24
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_category__error_count
  , sum(
    case
      when bid_error.error_code = 25
        then 1
      else 0
    end
  )                                                   as bid_rejected_cache_id__error_count
  , sum(
    case
      when bid_error.error_code = 26
        then 1
      else 0
    end
  )                                                   as bid_rejected_price__error_count
  , sum(
    case
      when bid_error.error_code = 27
        then 1
      else 0
    end
  )                                                   as bid_rejected_advertiser_blocklist__error_count
  , sum(
    case
      when bid_error.error_code = 28
        then 1
      else 0
    end
  )                                                   as bid_rejected_iab_cat_blocklist__error_count
  , sum(
    case
      when bid_error.error_code = 29
        then 1
      else 0
    end
  )                                                   as bid_rejected_adomain_missing__error_count
  , sum(
    case
      when bid_error.error_code = 30
        then 1
      else 0
    end
  )                                                   as bid_rejected_above_max_ad_duration__error_count
  , sum(
    case
      when bid_error.error_code = 31
        then 1
      else 0
    end
  )                                                   as bid_rejected_below_min_ad_duration__error_count
  , sum(
    case
      when bid_error.error_code = 32
        then 1
      else 0
    end
  )                                                   as no_media_file_satisfy_site_max_bitrate__error_count
  , sum(
    case
      when bid_error.error_code = 33
        then 1
      else 0
    end
  )                                                   as bid_rejected_pod_floor__error_count
  , sum(
    case
      when bid_error.error_code = 34
        then 1
      else 0
    end
  )                                                   as bid_rejected_all_house_ads__error_count
  , sum(
    case
      when bid_error.error_code = 35
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_ad_id__error_count
  , sum(
    case
      when bid_error.error_code = 36
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_creative_id__error_count
  , sum(
    case
      when bid_error.error_code = 37
        then 1
      else 0
    end
  )                                                   as no_media_file_satisfy_endpoint_max_bitrate__error_count
  , sum(
    case
      when bid_error.error_code = 38
        then 1
      else 0
    end
  )                                                   as no_media_file_satisfy_endpoint_min_bitrate__error_count
  , sum(
    case
      when bid_error.error_code = 39
        then 1
      else 0
    end
  )                                                   as no_media_file_satisfy_site_min_bitrate__error_count
  , sum(
    case
      when bid_error.error_code = 40
        then 1
      else 0
    end
  )                                                   as bid_rejected_brand_safety_rule_block__error_count
  , sum(
    case
      when bid_error.error_code = 41
        then 1
      else 0
    end
  )                                                   as bid_rejected_min_pod_size_not_met__error_count
  , sum(
    case
      when bid_error.error_code = 44
        then 1
      else 0
    end
  )                                                   as bid_price_missing__error_count
  , sum(
    case
      when bid_error.error_code = 45
        then 1
      else 0
    end
  )                                                   as bidder_not_shared_inventory__error_count
  , sum(
    case
      when bid_error.error_code = 46
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_adomain_elea__error_count
  , sum(
    case
      when bid_error.error_code = 47
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_adomain_click_through__error_count
  , sum(
    case
      when bid_error.error_code = 48
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_category_elea__error_count
  , sum(
    case
      when bid_error.error_code = 49
        then 1
      else 0
    end
  )                                                   as bid_rejected_price_above_error_threshold__error_count
  , sum(
    case
      when bid_error.error_code = 50
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_campaign_id__error_count
  , sum(
    case
      when bid_error.error_code = 51
        then 1
      else 0
    end
  )                                                   as bid_rejected_duplicate_apskey__error_count
  , sum(
    case
      when bid_error.error_code = 52
        then 1
      else 0
    end
  )                                                   as bid_rejected_mediafile_block_list__error_count
  , sum(
    case
      when bid_error.error_code = 53
        then 1
      else 0
    end
  )                                                   as bid_not_first_slot_in_pod__error_count
  , sum(
    case
      when bid_error.error_code = 54
        then 1
      else 0
    end
  )                                                   as bid_rejected_dup_mediafile_url_normal__error_count
  , sum(
    case
      when bid_error.error_code = 57
        then 1
      else 0
    end
  )                                                   as bid_rejected_slot_targeting_mismatch__error_count

from bid_error
left join demand_partner_map
  on bid_error.bidrequest_bids_headerbidder_id = demand_partner_map.bidder_id
left join publica_platform_map
  on bid_error.bidrequest_site_id = publica_platform_map.bidrequest_site_id
{{ dbt_utils.group_by(n=17) }}