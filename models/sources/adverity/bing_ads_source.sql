with

source as (

  select * from {{ source('adverity', 'bing_ads_campaigns') }}

)

, renamed as (

  select
    channel         as channel
    , ad_date::date as ad_date
    , campaign_name as ad
    , impressions   as impressions
    , conversions   as conversions
    , costs         as spend
  from source

)

select * from renamed