with

source as (

  select * from {{ source('adverity', 'mailgun_amazon_campaigns') }}

)

, renamed as (

  select
    channel         as channel
    , ad_date::date as ad_date
    , campaign_name as ad
    , impressions   as impressions
    , conversions   as conversions
    , cost          as spend
  from source
  where ad_date is not null

)

select * from renamed