{{ config(materialized='view') }}
with

all_adverity_campaigns as (
 {{ dbt_utils.union_relations(
      relations=[
        ref('liftoff_ads_stage')
        , ref('pinterest_ads_stage')
        , ref('bing_ads_stage')
        , ref('snapchat_ads_stage')
       
      ]
      , include=[
        "channel"
        , "ad_date"
        , "ad"
        , "spend"
        , "impressions"
        , "conversions"
      ]
    )
  }}

)

select
  ad_date
  , ad
  , spend
  , impressions
  , conversions
  , case when channel ilike 'liftoff'
      or channel ilike 'pinterest'
      or channel ilike 'snapchat'
      then 'acquisition_other'
    else channel
  end as channel
from all_adverity_campaigns