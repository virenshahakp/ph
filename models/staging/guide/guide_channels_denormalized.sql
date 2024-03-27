with

channels as (

  select * from {{ ref('guide_channels_stage') }}

)

select
  channels.asset_id            as asset_id
  , channels.asset_type        as asset_type
  , channels.callsign          as channel_callsign
  , channels.channel_id        as channel_id
  , channels.channel_name      as channel_name
  , channels.has_public_view   as has_public_view
  , channels.is_premium        as is_premium
  , channels.is_free           as is_free
from channels
