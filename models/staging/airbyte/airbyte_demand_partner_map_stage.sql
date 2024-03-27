--this is set up as an incremental model to help hedge against issues with the airbyte job

{{ config(
  materialized='incremental'
  , unique_key='publica_name'
  , tags=["dai", "exclude_hourly", "exclude_daily"] 
)
}}

select
  philo_name
  , publica_name
  , is_paid
  , is_count
from {{ ref('airbyte_demand_partner_map_source') }}



