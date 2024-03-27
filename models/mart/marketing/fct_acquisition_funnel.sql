{{ config(
    materialized = 'table'
    , sort = 'visited_at'
    , dist = 'account_id'
) }}

WITH

all_layers AS (

  {{ dbt_utils.union_relations(
      relations = [
        ref('acquisition_funnel_paid_layer_stage')
        , ref('acquisition_funnel_signed_up_layer_stage')
        , ref('acquisition_funnel_subscribed_layer_stage')
        , ref('acquisition_funnel_visit_layer_stage')
      ]
      , include = [
          "account_id"
          , "context_ip"
          , "context_campaign_source"
          , "context_campaign_name"
          , "context_campaign_term"
          , "context_campaign_medium"
          , "context_page_referrer"
          , "context_user_agent"
          , "context_page_path"
          , "context_campaign_content"
          , "context_campaign_content_id"
          , "visit_type"
          , "url"
          , "traffic_type"
          , "visit_platform"
          , "visit_platform_type"
          , "visit_platform_os"
          , "visit_browser"
          , "signup_platform"
          , "visited_at"
          , "signed_up_at"
          , "subscribed_at"
          , "first_paid_at"
          , "visit_anonymous_id"
          , "subscriber_billing"
          , "coupon_code"
          , "retarget"
          , "reference"
        ]
    )
  }}

)

SELECT
  account_id
  , context_ip
  , context_campaign_source
  , context_campaign_name
  , context_campaign_term
  , context_campaign_medium
  , context_page_referrer
  , context_user_agent
  , context_page_path
  , context_campaign_content
  , context_campaign_content_id
  , visit_type
  , url
  , traffic_type
  , visit_platform
  , visit_platform_type
  , visit_platform_os
  , visit_browser
  , signup_platform
  , visited_at
  , signed_up_at
  , subscribed_at
  , first_paid_at
  , visit_anonymous_id
  , subscriber_billing
  , coupon_code
  , reference
FROM
  all_layers
WHERE
  retarget = 1
