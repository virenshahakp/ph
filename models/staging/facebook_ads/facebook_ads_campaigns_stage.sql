with

campaigns as (

  select * from {{ ref('facebook_ads_campaigns_source') }}

)

select * from campaigns