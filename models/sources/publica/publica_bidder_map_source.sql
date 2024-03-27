{{ config(
  materialized='view'
 , tags=["dai", "exclude_hourly", "exclude_daily"]
) }}

with

bidder_map as (

  select * from {{ source('publica', 'bidder_map') }}

)

, renamed as (

  select
    bidder_id
    , bidder_name
  from bidder_map
  {{ dbt_utils.group_by(n=2) }}

)

select * from renamed
