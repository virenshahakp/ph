with

reports as (

  select
    id                 as ad_group_id
    , received_at
    , uuid
    , campaign_trial_type
    , date_start::date as ad_date
    , interactions
    , is_budget_explicitly_shared
    , video_quartile_25_rate
    , adwords_customer_id
    , all_conversion_rate
    , all_conversion_value
    , bounce_rate
    , campaign_status
    , conversions
    , active_view_impressions
    , amount
    , base_campaign_id
    , gmail_forwards
    , active_view_measurable_cost
    , average_cost
    , gmail_saves
    , gmail_secondary_clicks
    , interaction_types
    , uuid_ts
    , video_quartile_75_rate
    , video_views
    , advertising_channel_sub_type
    , view_through_conversions
    , invalid_clicks
    , video_quartile_100_rate
    , video_quartile_50_rate
    , video_view_rate
    , date_stop
    , budget_id
    , clicks
    , engagements
    , value_per_all_conversion
    , all_conversions
    , active_view_viewability
    , average_time_on_site
    , campaign_id
    , conversion_value
    , cost
    , impression_assisted_conversions
    , active_view_measurability
    , impressions
    , click_assisted_conversions
    , active_view_measurable_impressions
  from {{ source('google_ads', 'campaign_performance_reports') }}

)

select * from reports