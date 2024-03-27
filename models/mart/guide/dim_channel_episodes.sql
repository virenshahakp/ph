{{ config(materialized='table', sort='channel_episode_id', dist='ALL') }}

with

all_guide_data as (

  select * from {{ ref('dim_guide_metadata' ) }}

)

select distinct
  channel_id
  , channel_callsign
  , channel_name
  , show_title
  , show_id
  , episode_title
  , episode_id
  , original_air_date
  , content_type
  , is_premium
  , is_free
  , has_public_view
  , root_show_id
  , tms_series_id  
  , philo_series_id
  , series_title
  , show_episode_id
  , channel_episode_id
from all_guide_data