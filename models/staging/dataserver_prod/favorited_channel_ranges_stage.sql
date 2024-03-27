with

favorites_unfavorites as (

  select * from
    {{ dbt_utils.union_relations(
        relations=[
          ref('dataserver_prod_channel_favorite_stage')
          , ref('dataserver_prod_channel_unfavorite_stage')
        ],
        include=[
          "user_id"
          , "channel_id"
          , "event_type"
          , "event_timestamp"
        ]
      ) 
    }}

)

, favorite_unfavorited_sequence as (

  select
    *
    , lag(event_type) over (partition by user_id, channel_id order by event_timestamp asc)  as previous_event_type
    , lead(event_type) over (partition by user_id, channel_id order by event_timestamp asc) as next_event_type
    , case when
        event_type = 'favorite'
        then lead(event_timestamp) over (partition by user_id, channel_id order by event_timestamp asc)
    end                                                                                     as unfavorited_at
  from favorites_unfavorites

)

select
  user_id
  , channel_id
  , event_timestamp as favorited_at
  , unfavorited_at
from favorite_unfavorited_sequence
where event_type = 'favorite'

