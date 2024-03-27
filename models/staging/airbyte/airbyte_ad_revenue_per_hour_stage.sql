with

ad_rev_per_hour as (

  select * from {{ ref('airbyte_ad_revenue_per_hour_source') }}

)

select * from ad_rev_per_hour
