with

ads as (

  select * from {{ ref('facebook_ads_ads_source') }}

)

select * from ads