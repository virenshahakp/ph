--this is set up as an incremental model to help hedge against issues with the airbyte job

{{ config(
  materialized='incremental'
  , unique_key='demand_partner'
  , tags=["dai", "exclude_hourly", "exclude_daily"] )
}}


select
  demand_partner
  , diversity_type
from {{ ref('airbyte_demand_partner_diversity_source') }}


