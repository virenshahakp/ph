--this is set up as an incremental model to help hedge against issues with the airbyte job

{{ config(
  materialized='incremental'
  , unique_key='publica_name'
  , tags=["dai", "exclude_hourly", "exclude_daily"] 
)
}}

with

demand_partner_map as (

  select * from {{ source('airbyte', 'demand_partner_map') }}

)

, renamed as (

  select distinct
    "philo name"      as philo_name
    , "publica name"  as publica_name
    , lower("paid?")  as is_paid
    , lower("count?") as is_count
  from demand_partner_map

)

select * from renamed
