{{ config(materialized = 'table', sort = ['channel', 'ad_date']) }}
with 

ads as (

  select 
    ad_date 
    , campaign_id
    , sum(cost / 1000000.0) as marketing_costs 
    , sum(impressions) as impressions
    , sum(conversions) as conversions
    , sum(conversion_value) as revenue
  from {{ ref('google_ads_campaign_performance_reports_stage') }}
  where impressions > 0
  {{ dbt_utils.group_by(n=2) }}

)

, campaign_names as (

  select
    campaign_id 
    , start_date
    , end_date
    , campaign_name
  from {{ ref('google_ads_campaigns_stage') }}

)

, campaign as (

  select
    ads.ad_date
    , campaign_names.campaign_name as ad
    , ads.campaign_id
    , ads.impressions
    , ads.marketing_costs
    , ads.conversions
    , ads.revenue
  from ads 
  left join campaign_names on (campaign_names.campaign_id = ads.campaign_id)

)

select
  ad_date 
  , ad
  , case 
    when ad ilike '%Philo_AncillaryBrand%' 
      or ad ilike '%Philo_TopBrand%' 
      or ad ilike '%283 Brand Target CPA%' 
      or ad ilike '%Ancillary Brand S2S Event Test%' 
      or ad ilike '%ADC_Top Brand Terms_BMM%'
      or ad ilike '%A280 Brand Max Clicks%'
      or ad ilike '%Top Brand Impression Share Bid%'
      or ad ilike '%A280 Brand Max Clicks%'
      or ad ilike '%Philo_TopBrand_Territories%'
      then 'google_brand'
    when ad ilike '%youtube%'
      then 'youtube'
    else 'google_non_brand'
  end as channel
  , sum(campaign.marketing_costs) as spend
  , sum(impressions) as impressions
  , sum(conversions) as conversions
from campaign
where ad not ilike '%perfmax%' 
  and ad not ilike '%uac%'
{{ dbt_utils.group_by(n=3) }}



