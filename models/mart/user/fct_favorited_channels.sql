{{ config(materialized='table', dist='user_id', sort=['channel_id', 'favorited_at', 'unfavorited_at']) }}

with 

favorite_unfavorite_channels as (

  select * from {{ ref('favorited_channel_ranges_stage') }}
  
)

select 
  user_id
  , channel_id
  , favorited_at
  , unfavorited_at
from favorite_unfavorite_channels
