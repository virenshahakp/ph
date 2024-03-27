{{ config(
  materialized='view'
 , tags=["dai", "exclude_hourly", "exclude_daily"]
) }}

select
  ts::date                               as partition_date
  , extract(hour from ts)::int           as partition_hour
  , bidder_type
  , is_d_d_monitoring_enabled
  , s_o_v_parallel_auctions_backfill_enabled
  , s_o_v_parallel_auctions_enabled
  , active_bidder_tier
  , ad_break_id                          as adbreak_id
  , ad_break_position
  , app_bundle_id
  , app_name
  , aps_bids_submitted
  , atts_str
  , auction_requester
  , auction_uuid
  , bid_error_debug
  , bid_request__bids__code
  , bid_request__bids__custom_bidder_name
  , bid_request__bids__headerbidder_id   as bidrequest_bids_headerbidder_id
  , bid_request__device__devicetype
  , bid_request__device__geo__accuracy
  , bid_request__device__geo__city       as bidrequest_device_geo_city
  , bid_request__device__geo__country    as bidrequest_device_geo_country
  , bid_request__device__geo__ipservice
  , bid_request__device__geo__metro::int as bidrequest_device_geo_metro
  , bid_request__device__geo__region     as bidrequest_device_geo_region
  , bid_request__device__geo__type
  , bid_request__device__geo__zip        as bidrequest_device_geo_zip
  , bid_request__device__h
  , bid_request__device__ip
  , bid_request__device__make
  , bid_request__device__model
  , bid_request__device__os
  , bid_request__device__osv
  , bid_request__device__ppi
  , bid_request__device__ua
  , bid_request__device__w
  , bid_request__id
  , bid_request__site__id                as bidrequest_site_id
  , bid_request__site__publisher__id
  , bid_uuid
  , bidder_sub_tier
  , bidder_tier::int
  , ccpa
  , ccpa_str
  , consent
  , consent_str
  , content__channel                     as content_channel
  , content__contentrating
  , content__context
  , content__genre
  , content__id
  , content__language
  , content__len
  , content__network                     as content_network
  , content__producer__id
  , content__series
  , content_id
  , content_producer_id
  , coppa
  , creation_timestamp
  , custom__adomain
  , custom__cache
  , custom__cat
  , custom__cb
  , custom__custom1
  , custom__custom3
  , custom__host
  , custom__largest_mf_url
  , custom__normalized_largest_mf_url
  , deal_id
  , destination_site_id
  , destination_type
  , device_id
  , device_id_freq_cap
  , dnt
  , dnt_str
  , endpoint_uuid
  , gdpr_consent
  , gdpr_str
  , global_timeout
  , ias_contextual_info
  , ifv_enabled
  , inventory_type
  , ip
  , is_multi_bid
  , is_podded_request
  , is_shared_inventory
  , livestream::int
  , livestream_str
  , lmt
  , lmt_str
  , multiplier_amount
  , number_of_ads_requested::int
  , pbs_cluster
  , pbs_region
  , pod_number
  , publica_deal_ids
  , publisher_name
  , publisher_uuid
  , requested_pod_duration::int
  , roku_ad_keys
  , sampling_fraction
  , segment_ids
  , session_id
  , share_of_voice
  , share_of_voice_type
  , should_obfuscate_ip
  , site_i_d
  , site_name
  , site_page
  , site_uuid
  , spotx_param_channel_id
  , synthetic_id
  , targeting_device
  , targeting_device_subtype
  , type
  , unpaid_bidder_enabled
  , vmap_uuid
  , ts
  , bid_request__device__language
  , custom_targeting
  , custom__custom6
  , custom__custom4
  , custom__custom5
  , demand_side_platform_i_d
  , advertiser_i_d
  , a_p_s_key
  , custom__parse_params_duration
  , case
    when livestream = 1 then 'live'
    when livestream = 2 then 'vod'
    when livestream = 3 then 'dvr'
  end                                    as asset_type
from {{ source('publica', 'no_bid_dyn') }}