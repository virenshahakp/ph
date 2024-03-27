with

campaigns as (

  select * from {{ ref('cj_ads_source') }}

)

select * from campaigns