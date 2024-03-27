with source as (
  select * from {{ source('adverity', 'cjaffiliate_campaigns') }}
)

, renamed as (

  select
    channel                  as channel
    , ad_date::date          as ad_date
    , campaign_name          as ad
    , impressions            as impressions
    , conversions            as conversions
    , sum(fees_other + cost) as spend
  from source
  {{ dbt_utils.group_by(n=5) }}
)

select * from renamed