{% set idfa_date = "2021-05-01" %}
with

insights as (

  select * from {{ ref('facebook_ads_insights_stage') }}

)

, ads as (

  select * from {{ ref('facebook_ads_ads_stage') }}

)

, campaigns as (

  select * from {{ ref('facebook_ads_campaigns_stage') }}

)

select
  insights.ad_date
  , campaigns.campaign_name          as ad
  , insights.spend
  , insights.impressions
  , insights.inline_post_engagements as conversions
  , case
    when campaigns.campaign_name ilike '%retargeting%' and insights.ad_date < '{{ idfa_date }}'
      then 'fb_retargeting_pre_idfa'
    when campaigns.campaign_name ilike '%show%' and insights.ad_date < '{{ idfa_date }}'
      then 'fb_prospecting_show_pre_idfa'
    when campaigns.campaign_name ilike '%retargeting%' and insights.ad_date >= '{{ idfa_date }}'
      then 'fb_retargeting_post_idfa'
    when campaigns.campaign_name ilike '%show%' and insights.ad_date >= '{{ idfa_date }}'
      then 'fb_prospecting_show_post_idfa'
    when insights.ad_date < '{{ idfa_date }}'
      then 'fb_prospecting_on_pre_idfa'
    else 'fb_prospecting_on_post_idfa'
  end                                as channel
from insights
left join ads on (insights.ad_id = ads.ad_id)
left join campaigns on (ads.campaign_id = campaigns.campaign_id)