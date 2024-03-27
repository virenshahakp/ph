with

daily_tv as (

  select 
    ad
    , ad_date
    , channel
    , spend
    , impressions
  from {{ ref('uploads_daily_tv_performance_source') }}

)

select * from daily_tv