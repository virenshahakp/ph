with

campaign as (

  select * from {{ ref('braze_prod_campaign_converted_source') }}

)

select * from campaign
