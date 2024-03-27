with

app_installs as (

  select * from {{ ref('android_prod_application_installed_source') }}

)

, appsflyer as (

  select distinct
    context_device_advertising_id
    , media_source
    , attributed_touch_time
    , attributed_touch_type
    , campaign
  from {{ ref('appsflyer_google_installs_stage') }}

)

select
  app_installs.event_id
  , app_installs.anonymous_id
  , app_installs.user_id
  , app_installs.context_ip
  , app_installs.context_campaign_term
  , app_installs.context_campaign_medium
  , app_installs.context_page_referrer
  , app_installs.context_user_agent
  , app_installs.context_page_path
  , app_installs.url
  , app_installs.context_campaign_content
  , app_installs.context_campaign_content_id
  , app_installs.visited_at
  , app_installs.priority
  , app_installs.visit_type
  , app_installs.coupon_code
  , app_installs.context_device_advertising_id
  , app_installs.reference
  , appsflyer.attributed_touch_time
  , appsflyer.attributed_touch_type
  , appsflyer.media_source as context_campaign_source
  , appsflyer.campaign     as context_campaign_name
from app_installs
left join appsflyer on (
  app_installs.context_device_advertising_id = appsflyer.context_device_advertising_id
  and appsflyer.attributed_touch_type != ''
)
