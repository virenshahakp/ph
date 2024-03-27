{{ config(
  materialized='view'
 , tags=["dai", "exclude_hourly", "exclude_daily"]
) }}


select
  ts::date                                   as partition_date
  , extract(hour from ts)::int               as partition_hour
  , ts
  , bidder_type
  , is_d_d_monitoring_enabled
  , active_bidder_tier::int
  , ad_break_id                              as adbreak_id
  , ad_break_position::int
  , ad_duration::int
  , app_bundle_id
  , app_name
  , aps_bids_submitted
  , atts_str
  , auction_requester
  , auction_uuid
  , bid_error_debug
  , bid_floor::decimal(19, 8)
  , bid_gross_price::decimal(19, 8)
  , bid_uuid
  , bidder_sub_tier
  , bidder_tier::int
  , brand_safety_rule_id
  , campaign_id
  , ccpa
  , ccpa_str
  , consent
  , consent_str
  , content_id
  , content_producer_id
  , coppa
  , creation_timestamp::timestamp
  , deal_id
  , destination_site_id
  , destination_type
  , device_id
  , device_id_freq_cap
  , dnt
  , dnt_str
  , dsp
  , enable_save_creative_validations
  , endpoint_uuid
  , error_code
  , gdpr_consent
  , gdpr_str
  , global_timeout::int
  , ias_contextual_info
  , ifv_enabled
  , ip
  , is_multi_bid
  , is_podded_request
  , largest_mf_url
  , livestream::int
  , lmt
  , lmt_str
  , multiplier_amount::decimal(19, 8)
  , normalized_largest_mf_url
  , number_of_ads_requested::int
  , order_id
  , philo_fallback
  , pod_number
  , publica_deal_ids
  , publisher_name
  , publisher_uuid
  , requested_pod_duration::int
  , roku_ad_keys
  , sampling_fraction::decimal(19, 8)
  , segment_ids
  , session_id
  , share_of_voice::decimal(19, 8)
  , site_i_d                                 as site_id
  , site_name
  , site_page
  , site_uuid
  , spotx_endpoint
  , spotx_param_channel_id
  , synthetic_id
  , targeting_device
  , targeting_device_subtype
  , type
  , unpaid_bidder_enabled
  , vmap_uuid
  , bid_request__id
  , bid_response__ad_id
  , bid_response__ad_unit_code
  , bid_response__bidder
  , bid_response__cpm::decimal(19, 8)        as bidresponse_cpm
  , bid_response__creative_id                as bidresponse_creative_id
  , bid_response__currency
  , bid_response__headerbidder_id
  , bid_response__height
  , bid_response__id
  , bid_response__media_type
  , bid_response__time_to_respond::int
  , bid_response__width
  , content__channel                         as content_channel
  , content__contentrating
  , content__context
  , content__genre
  , content__id
  , content__language
  , content__len::int
  , content__network                         as content_network
  , content__series
  , content__title
  , custom__adomain                          as custom_adomain
  , custom__cache
  , custom__cat
  , custom__cb
  , custom__click_through_adomains
  , custom__click_through_url
  , custom__custom1
  , custom__custom10
  , custom__custom11
  , custom__custom12
  , custom__custom13
  , custom__custom14
  , custom__custom15
  , custom__custom16
  , custom__custom17
  , custom__custom18
  , custom__custom2
  , custom__custom3
  , custom__custom4
  , custom__custom5
  , custom__custom6
  , custom__custom7
  , custom__custom8
  , custom__custom9
  , custom__debug_10_get_device_segments
  , custom__debug_11_get_device_idl
  , custom__debug_12_get_campaign_pacing
  , custom__debug_13_get_device_or_ip
  , custom__debug_14_get_user_sync
  , custom__debug_15_get_publica_deals
  , custom__debug_16_parse_params_end
  , custom__debug_17_create_bid_request
  , custom__debug_18_start_hold_auction
  , custom__debug_19_clean_open_rtb_requests
  , custom__debug_1_parse_standard_params_start
  , custom__debug_20_add_uuid_to_reqw
  , custom__debug_21_randomize_list
  , custom__debug_22_reqw_bidrequest_ext_unmarshal
  , custom__debug_23_run_auction_start
  , custom__debug_24_make_auction_context
  , custom__debug_2_parse_site_unix
  , custom__debug_3_parse_user_agent
  , custom__debug_4_fox_regex_unix
  , custom__debug_5_create_device_uuid
  , custom__debug_6_ip_geo
  , custom__debug_7_parse_app_site_params
  , custom__debug_8_unmarshal_content_ext
  , custom__debug_9_get_iris_segments
  , custom__error_message
  , custom__host
  , custom__mf_normalized
  , custom__mf_original
  , custom__rejected_mf
  , recognition_result__adomain_enriched_by
  , recognition_result__adomain_error_message
  , recognition_result__category_enriched_by
  , recognition_result__category_error_message
  , recognition_result__enriched_adomain
  , recognition_result__enriched_adomain_input
  , recognition_result__enriched_category
  , recognition_result__enriched_category_input
  , recognition_result__has_error
  , recognition_result__has_result
  , recognition_result__replaced_adomain
  , bid_request__device__devicetype
  , bid_request__device__h
  , bid_request__device__ip
  , bid_request__device__make
  , bid_request__device__model
  , bid_request__device__os
  , bid_request__device__osv
  , bid_request__device__ppi
  , bid_request__device__ua
  , bid_request__device__w
  , bid_request__site__id                    as bidrequest_site_id
  , content__producer__id
  , bid_request__bids__code
  , bid_request__bids__custom_bidder_name
  , bid_request__bids__headerbidder_id       as bidrequest_bids_headerbidder_id
  , bid_request__device__geo__accuracy
  , bid_request__device__geo__city           as bidrequest_device_geo_city
  , bid_request__device__geo__country        as bidrequest_device_geo_country
  , bid_request__device__geo__ipservice
  , bid_request__device__geo__metro::int     as bidrequest_device_geo_metro
  , bid_request__device__geo__region         as bidrequest_device_geo_region
  , bid_request__device__geo__zip            as bidrequest_device_geo_zip
  , bid_request__site__publisher__id
  , bid_request__device__geo__type
  , should_obfuscate_ip
  , ab_testing_selected_config_id
  , ab_testing_selected_test_id
  , custom_targeting
  , pbs_region
  , custom__largest_mf_url
  , pbs_cluster
  , custom__normalized_largest_mf_url
  , is_shared_inventory
  , share_of_voice_type
  , s_o_v_parallel_auctions_enabled          as sov_parallel_auctions_enabled
  , inventory_type
  , s_o_v_parallel_auctions_backfill_enabled as sov_parallel_auctions_backfill_enabled
  , channel_ab_testing_selected_config_id
  , channel_ab_testing_selected_test_id
  , bid_request__device__language
  , recognition_result__detected_language
  , recognition_result__enriched_adomain_date_time
  , demand_side_platform_i_d                 as demand_side_platform_id
  , advertiser_i_d                           as advertiser_id
  , a_p_s_key                                as aps_key
  , recognition_result__replaced_category
  , null::boolean                            as recognition_is_missing_adomain
  -- The following fields are no longer sent along
  -- in this event but are used downstream 
  , null::boolean                            as recognition_is_missing_category
  , case
    when livestream = 1 then 'live'
    when livestream = 2 then 'vod'
    when livestream = 3 then 'dvr'
  end                                        as asset_type
from {{ source('publica', 'bid_error_dyn') }}
-- where ts >= '2023-06-14'

