with

campaigns as (

  select * from {{ ref('google_ads_campaigns_source') }}

)

select * from campaigns