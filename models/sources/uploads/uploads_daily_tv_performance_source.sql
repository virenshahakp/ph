with

daily_marketing as (

  select
    ad
    , spot_date::date      as ad_date
    , channel              as channel
    , cost::decimal        as spend
    , impressions::decimal as impressions
  from {{ source('uploads', 'daily_tv_performance') }}
  where channel is not null

)

select * from daily_marketing