with

ad_revenue as (

  select * from {{ source('uploads', 'ad_revenue_historic') }}

)

, renamed as (

  select
    impression_date
    , impressions
    , revenue
    , provider
  from ad_revenue
  where
    revenue is not null
    and trim(revenue) != ''
    and revenue > 0
    and impressions is not null
    and trim(impressions) != ''
    and impressions > 0

)

select * from renamed
