{{ config(materialized='view') }}


select 
  partition_date 
  , ad_break_id
  , partition_hour
  , asset_type 
  , network 
  , channel 
  , callsign 
  , ad_break_duration 
  , is_dai_slot 
  , dai_slot_type
  , dai_slot_owner 
  , ad_break_received_at 
  , ad_break_created_at 
  , asset_id 
  , ad_break_start_ts 
  , ad_break_end_ts 
from {{ ref('tbl_ad_breaks') }}