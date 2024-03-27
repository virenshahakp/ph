{{ config(
materialized='incremental'
, unique_key='bidder_id'
, tags=["dai", "exclude_hourly", "exclude_daily"] 
)
}}

select
  bidder_id
  , bidder_name
from {{ ref('publica_bidder_map_source') }}