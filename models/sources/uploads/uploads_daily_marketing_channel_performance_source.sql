with

daily_marketing as (

  select
    ad
    , date::date           as ad_date
    , spend::decimal       as spend
    , impressions::decimal as impressions
    , conversions          as conversions
    , lower(channel)       as channel
  from {{ source('uploads', 'daily_marketing_channel_performance') }}
  where channel is not null

)

select * from daily_marketing