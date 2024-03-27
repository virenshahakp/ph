with

app_installs as (

  select * from {{ ref('ios_prod_application_installed_source') }}

)

, appsflyer as (

  select distinct
    media_source
    , ad
    , idfa
    , device_id
    , attributed_touch_time
    , attributed_touch_type
    , install_time
    , campaign
  from {{ ref('appsflyer_ios_installs_stage') }}

)

, standard_installs as (

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
    , app_installs.context_device_id
    , app_installs.context_device_advertising_id
    , app_installs.reference
    , appsflyer.attributed_touch_time
    , appsflyer.attributed_touch_type
    , appsflyer.campaign as context_campaign_name
    -- facebook has introduced automated ads.  We want to evaluate these
    -- ads separately from our normal facebook app install campaigns
    -- when we see an appsflyer_ios_installs_stage.ad = 'Automated Asset'
    -- we should adjust the campaign source and to reflect this as a facebook automated ad.
    , case
      when appsflyer.ad = 'Automated Asset'
        then 'Facebook Automated Asset Ad'
      else appsflyer.media_source
    end                  as context_campaign_source
  from app_installs
  -- apple will set the idfa to be all 0s if ad tracking is off.  
  -- Previous logic looked for matches using the idfa value
  -- or the device id. lots of users have ad tracking off, 
  -- and > 99% join on the device_id so we removed the idfa join for
  -- performance benefits
  left join appsflyer on (
    app_installs.context_device_id = appsflyer.device_id
    and appsflyer.attributed_touch_type != ''
  )

)

, apple_fix as (

  select * from {{ ref('apple_anonymous_id_install_fix_2022_07_05') }}

)

, missing_installs as (

  select
    app_installs.event_id
    , apple_fix.anonymous_id
    , null               as user_id -- LB: apple bug made app install events have a user_id
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
    , app_installs.context_device_id
    , app_installs.context_device_advertising_id
    , app_installs.reference
    , appsflyer.attributed_touch_time
    , appsflyer.attributed_touch_type
    , appsflyer.campaign as context_campaign_name
    -- facebook has introduced automated ads.  We want to evaluate these
    -- ads separately from our normal facebook app install campaigns
    -- when we see an appsflyer_ios_installs_stagee.ad = 'Automated Asset'
    -- we should adjust the campaign source and to reflect this as a facebook automated ad.
    , case
      when appsflyer.ad = 'Automated Asset'
        then 'Facebook Automated Asset Ad'
      else appsflyer.media_source
    end                  as context_campaign_source
  from apple_fix
  inner join app_installs
    on (
      apple_fix.event_id = app_installs.event_id
    )
  left join appsflyer
    on (
      app_installs.context_device_id = appsflyer.device_id
      and appsflyer.attributed_touch_type != ''
    )
  where apple_fix.platform = 'ios'

)

select * from missing_installs
union -- noqa: L033
select * from standard_installs
