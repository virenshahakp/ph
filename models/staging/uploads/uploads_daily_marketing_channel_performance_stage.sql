with 

daily_marketing as (

  select
    ad
    , ad_date
    , channel
    , spend
    , impressions
    , conversions
  from {{ ref('uploads_daily_marketing_channel_performance_source') }}

)

select * from daily_marketing