with

campaigns as (

  select * from {{ ref('snapchat_ads_source') }}

)

select * from campaigns