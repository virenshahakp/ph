{{
  config(
    materialized='incremental'
    , dist='anonymous_id'
    , sort=['anonymous_id', 'visited_at']
    , on_schema_change= 'append_new_columns'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

launch as (

  select * from {{ ref('roku_prod_launch_source') }}

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
  where app_id = 'id1248646044'

)

select
  launch.event_id
  , launch.anonymous_id
  , launch.loaded_at
  , launch.context_ip
  , launch.context_campaign_term
  , launch.context_campaign_medium
  , launch.context_page_referrer
  , launch.context_user_agent
  , launch.context_page_path
  , launch.context_campaign_content
  , launch.context_campaign_content_id
  , launch.url
  , launch.visited_at
  , launch.coupon_code
  , 'roku'                                                           as visit_type
  , launch.reference
  , launch.has_fast_account_request
  , appsflyer.attributed_touch_time
  , appsflyer.attributed_touch_type
  , case
    when launch.source like 'ad%' and launch.context_campaign_source is null then 1
    when launch.context_campaign_source is not null then 1
    else 2
  end                                                                as priority
  , coalesce(appsflyer.media_source, launch.context_campaign_source) as context_campaign_source
  , coalesce(appsflyer.campaign, launch.context_campaign_name)       as context_campaign_name
from launch
left join appsflyer
  on (
    launch.context_device_advertising_id = appsflyer.context_device_advertising_id
    and appsflyer.attributed_touch_type != ''
  )
{%- if is_incremental() %}
  where launch.loaded_at > {{ max_loaded_at }}
{%- endif %}