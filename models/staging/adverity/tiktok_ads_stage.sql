with

campaigns as (

  select * from {{ ref('tiktok_ads_source') }}

)

select * from campaigns