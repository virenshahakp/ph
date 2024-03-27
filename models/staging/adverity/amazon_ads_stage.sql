with campaign as (

  select * from {{ ref('amazon_ads_source') }}

)

select * from campaign