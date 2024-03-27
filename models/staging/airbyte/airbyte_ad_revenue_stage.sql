{{ 
  config(
    materialized = 'table',
    dist = 'AUTO',
    sort = ['impression_date', 'provider']
  )
}}


with

ad_revenue as (

  select * from {{ ref('airbyte_ad_revenue_source') }}

)

select * from ad_revenue
