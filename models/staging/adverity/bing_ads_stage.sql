with

campaigns as (

  select * from {{ ref('bing_ads_source') }}

)

select * from campaigns