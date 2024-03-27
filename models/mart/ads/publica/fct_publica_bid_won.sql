{{ config(materialized='view') }}

select
  publica_bid_won_dyn_source.*
  , dim_publica_platform_map.client_type
  , dim_demand_partner_map.bidder_name
  , dim_demand_partner_map.demand_partner
  , dim_demand_partner_map.fill_type_classification
  , dim_demand_partner_map.pod_owner
  , dim_demand_partner_map.is_paid
  , dim_demand_partner_map.is_count
  , dim_demand_partner_map.diversity_type
from {{ ref('publica_bid_won_dyn_source') }}
left join {{ ref('dim_demand_partner_map') }}
  on publica_bid_won_dyn_source.bidrequest_bids_headerbidder_id = dim_demand_partner_map.bidder_id
left join {{ ref('dim_publica_platform_map') }}
  on publica_bid_won_dyn_source.bidrequest_bids_headerbidder_id = dim_publica_platform_map.bidrequest_site_id