{{ config(materialized='view') }}

with

all_adverity_campaigns as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('apple_ads_stage')
        , ref('cj_ads_stage')
        , ref('tiktok_ads_stage')
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

select * from all_adverity_campaigns