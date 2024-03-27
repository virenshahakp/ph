with

campaigns as (

  select * from {{ ref('liftoff_ads_source') }}

)

select * from campaigns