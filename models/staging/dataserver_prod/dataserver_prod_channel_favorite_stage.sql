with

channel_favs as (

  select * from {{ ref('dataserver_prod_channel_favorite_source') }}

)

select
  channel_favs.*
  , 'favorite' as event_type
from channel_favs
