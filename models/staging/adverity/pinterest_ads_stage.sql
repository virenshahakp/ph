with

campaigns as (

  select * from {{ ref('pinterest_ads_source') }}

)

select * from campaigns