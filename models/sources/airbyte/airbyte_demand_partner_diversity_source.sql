{{ config(
  materialized='incremental'
  , unique_key='demand_partner'
  , tags=["dai", "exclude_hourly", "exclude_daily"] )
}}

with

demand_partner_diversity_map as (

  select * from {{ source('airbyte', 'demand_partner_diversity_map') }}

)

, renamed as (

  select distinct
    partner as demand_partner
    , type  as diversity_type
  from demand_partner_diversity_map

)

select * from renamed
