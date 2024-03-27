with

channel_unfavs as (

  select * from {{ ref('dataserver_prod_channel_unfavorite_source') }}

)

select
  channel_unfavs.*
  , 'unfavorite' as event_type
from channel_unfavs
