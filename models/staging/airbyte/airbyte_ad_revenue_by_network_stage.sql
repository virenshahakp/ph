with

ad_revenue as (

  select * from {{ ref('airbyte_ad_revenue_by_network_source') }}

)

select * from ad_revenue
