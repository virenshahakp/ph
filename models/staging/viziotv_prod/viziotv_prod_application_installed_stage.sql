with

app_installs as (

  select * from {{ ref('viziotv_prod_application_installed_source') }}

)

, appsflyer as (
  select distinct
    channel
    , media_source
    , context_device_advertising_id
    , attributed_touch_time
    , attributed_touch_type
    , campaign
  from {{ ref('spectrum_appsflyer_installs_stage') }}
  where app_id = 'vzphilo'

)


select
  app_installs.event_id
  , app_installs.anonymous_id
  , app_installs.user_id
  , app_installs.context_ip
  , app_installs.context_user_agent
  , app_installs.context_page_path
  , app_installs.context_page_title
  , app_installs.url
  , app_installs.visited_at
  , app_installs.priority
  , app_installs.visit_type
  , app_installs.coupon_code
  , app_installs.context_device_advertising_id
  , app_installs.reference
  , appsflyer.attributed_touch_time
  , appsflyer.attributed_touch_type
  , appsflyer.campaign     as context_campaign_name
  , appsflyer.media_source as context_campaign_source

from app_installs
left join appsflyer on (
  app_installs.context_device_advertising_id = appsflyer.context_device_advertising_id
  and appsflyer.attributed_touch_type != ''
)
