with

daily_uploads as (

  select * from {{ ref('uploads_daily_marketing_channel_performance_stage') }}

)

select 
  ad_date
  , ad
  , spend
  , impressions
  , conversions
  , lower(channel) as channel
from daily_uploads
where 
  channel not ilike 'google'
  and channel not ilike 'google_discovery'
  and channel not ilike 'google_app'
  and channel not ilike 'bing'
  and channel not ilike 'tiktok'
  and channel not ilike 'cj'
  and channel not ilike 'snapchat'
