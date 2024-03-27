with

campaign as (

  select * from {{ ref('braze_prod_campaign_control_group_entered_source') }}

)

select * from campaign
