with

all_mmm_spend as (

  {{ dbt_utils.union_relations(
      relations=[
        ref('marketing_daily_spend_mmm_acquisition_other_stage')
        , ref('marketing_daily_spend_mmm_adverity_other')
        , ref('marketing_daily_spend_mmm_adverity')
        , ref('marketing_daily_spend_mmm_facebook')
        , ref('marketing_daily_spend_mmm_google_brand_non_brand')
        , ref('marketing_daily_spend_mmm_google_other')
        , ref('marketing_daily_spend_mmm_tv')
        , ref('marketing_daily_spend_mmm_uploads_excluding_google')
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

select * from all_mmm_spend