with

insights as (

  select
    id           as campaign_id
    , received_at
    , uuid
    , date_stop
    , unique_impressions
    , ad_id
    , date_start as ad_date
    , frequency
    , reach
    , social_spend
    , clicks
    , inline_post_engagements
    , link_clicks
    , uuid_ts
    , impressions
    , unique_clicks
    , spend
  from {{ source('facebook_ads', 'insights') }}

)

select * from insights