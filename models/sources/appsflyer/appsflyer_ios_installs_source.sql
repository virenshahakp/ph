with

installs as (

  select * from {{ source('appsflyer', 'ios_installs') }}

)

, renamed as (

  select -- noqa: L034
    attributed_touch_type
    , attributed_touch_time
    , install_time
    , event_time
    , event_name
    , campaign
    , campaign_id
    , adset
    , adset_id
    , ad
    , ad_id
    , ad_type
    , site_id
    , appsflyer_id
    , advertising_id   as context_device_advertising_id
    , idfa
    , lower(device_id) as device_id
    , media_source
    , channel
    , keywords
  from installs

)

select * from renamed
