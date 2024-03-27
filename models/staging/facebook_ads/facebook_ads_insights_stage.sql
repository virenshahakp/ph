with

insights as (

  select * from {{ ref('facebook_ads_insights_source') }}

)

select * from insights