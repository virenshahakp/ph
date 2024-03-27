{{ config(materialized='table', sort='channel_id', dist='ALL') }}

with

all_guide_data as (

  select * from {{ ref('dim_guide_metadata' ) }}

)

, media_groups as (

  select * from {{ ref('dim_media_groups') }}

)

select distinct
  all_guide_data.channel_id
  , all_guide_data.channel_callsign
  , all_guide_data.channel_name
  , all_guide_data.has_public_view
  , all_guide_data.is_premium
  , all_guide_data.is_free
  , media_groups.media_group
from all_guide_data
left join media_groups on (all_guide_data.channel_callsign = media_groups.callsign)