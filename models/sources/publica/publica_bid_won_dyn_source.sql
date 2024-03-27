{{ config(
  materialized='view'
 , tags=["dai", "exclude_hourly", "exclude_daily"]
) }}

select
  ts::date                               as partition_date
  , extract(hour from ts)::int           as partition_hour
  , bidder_type
  , is_d_d_monitoring_enabled
  , active_bidder_tier
  , ad_break_id                          as adbreak_id
  , ad_break_position
  , ad_duration::int
  , app_bundle_id
  , app_name
  , aps_bids_submitted
  , atts_str
  , auction_requester
  , auction_uuid
  , bid_density_bucket
  , bid_error_debug
  , bid_floor
  , bid_gross_price
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
  , bid_response__ad_id
  , bid_response__ad_unit_code
  , bid_response__bidder
  , bid_response__cpm::decimal(19, 8)    as bidresponse_cpm
  , bid_response__creative_id            as bidresponse_creative_id
  , bid_response__currency
  , bid_response__headerbidder_id
  , bid_response__height
  , bid_response__id
  , bid_response__media_type
  , bid_response__time_to_respond
  , bid_response__width
  , bid_response_deal_id
  , bid_uuid
  , bid_won_uuid
  , bidder_sub_tier
  , bidder_tier::int
  , campaign_id
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
  , custom__adomain                      as custom_adomain
  , custom__cache
  , custom__cat
  , custom__cb
  , custom__click_through_adomains
  , custom__click_through_url
  , custom__custom1
  , custom__custom2
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
  , dsp
  , endpoint_uuid
  , gdpr_consent
  , gdpr_str
  , global_timeout
  , ias_contextual_info
  , ifv_enabled
  , ip
  , is_multi_bid
  , is_podded_request
  , largest_mf_url
  , livestream::int
  , livestream_str
  , lmt
  , lmt_str
  , multiplier_amount
  , normalized_largest_mf_url
  , number_of_ads_requested
  , order_id
  , pbs_cluster
  , pbs_region
  , philo_fallback
  , pod_number
  , publica_deal_ids
  , publisher_name
  , publisher_uuid
  , recognition_result__adomain_enriched_by
  , recognition_result__enriched_adomain
  , recognition_result__enriched_adomain_input
  , recognition_result__has_result
  , requested_pod_duration::int
  , roku_ad_keys
  , sampling_fraction
  , segment_ids
  , session_id
  , share_of_voice
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
  , recognition_result__enriched_category
  , recognition_result__category_enriched_by
  , recognition_result__adomain_error_message
  , recognition_result__category_error_message
  , recognition_result__enriched_category_input
  , recognition_result__has_error
  , is_shared_inventory
  , share_of_voice_type
  , s_o_v_parallel_auctions_enabled
  , inventory_type
  , s_o_v_parallel_auctions_backfill_enabled
  , recognition_result__detected_language
  , channel_ab_testing_selected_config_id
  , channel_ab_testing_selected_test_id
  , recognition_result__replaced_adomain
  , applied_contextual_segment_ids
  , applied_contextual_segment_partners
  , bid_request__device__language
  , custom_targeting
  , recognition_result__enriched_adomain_date_time
  , custom__custom6
  , custom__custom4
  , custom__custom5
  , demand_side_platform_i_d
  , advertiser_i_d
  , is_backfill_response
  , remaining_pod_duration
  , available_pod_duration
  , a_p_s_key
  , recognition_result__replaced_category
  , max_available_pod_duration_by_tier
  , custom__parse_params_duration
  , applied_segment_ids
  , case
    when livestream = 1 then 'live'
    when livestream = 2 then 'vod'
    when livestream = 3 then 'dvr'
  end                                    as asset_type
from {{ source('publica', 'bid_won_dyn') }}