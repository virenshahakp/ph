with

campaigns as (

  select * from {{ ref('apple_ads_source') }}

)

select * from campaigns
where ad_date > '2022-10-31'