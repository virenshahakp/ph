--this is set up as an incremental model to help hedge against issues with the airbyte job

{{ config(
  materialized='incremental'
  , unique_key='bidder_id'
  , sort='bidder_id'
  , dist='bidder_id' 
  , tags=["dai", "exclude_hourly", "exclude_daily"] 
) }}

with bidder_map as (
  select * from {{ ref('publica_bidder_map_stage') }}
)

, demand_partner_map as (
  select * from {{ ref('airbyte_demand_partner_map_stage') }}
)

, demand_partner_diversity_map as (
  select * from {{ ref('airbyte_demand_partner_diversity_stage') }}
)

select
  bidder_map.bidder_id
  , bidder_map.bidder_name
  , demand_partner_map.is_paid
  , demand_partner_map.is_count
  , demand_partner_map.philo_name                                            as demand_partner
  , demand_partner_diversity_map.diversity_type
  , case when demand_partner_map.philo_name is null then 'no' else 'yes' end as has_match_in_ad_pulse
  , case
    when demand_partner_map.is_paid ilike 'yes' then 'Paid'
    when demand_partner_map.is_paid ilike 'no' and lower(bidder_map.bidder_name) ilike '%marketing%' then 'Marketing'
    else 'Unpaid'
  end                                                                        as fill_type_classification
  , case when demand_partner_map.is_paid ilike 'no' and lower(bidder_map.bidder_name) ilike '%sov%' then 'Partner'
    else 'Philo'
  end                                                                        as pod_owner
from bidder_map
left join demand_partner_map
  on bidder_map.bidder_name = demand_partner_map.publica_name
left join demand_partner_diversity_map
  on demand_partner_map.philo_name = demand_partner_diversity_map.demand_partner
