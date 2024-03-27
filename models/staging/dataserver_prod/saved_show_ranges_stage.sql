with

saves_unsaves as (

  select *
  from
    {{ dbt_utils.union_relations(
        relations=[
          ref('dataserver_prod_show_save_stage')
          , ref('dataserver_prod_show_unsave_stage')
        ],
        include=[
          "user_id"
          , "show_id"
          , "event_type"
          , "timestamp"
        ]
      ) 
    }}

)

, save_unsave_sequence as (

  select
    *
    , lag(event_type) over (partition by user_id, show_id order by "timestamp" asc)  as previous_event_type
    , lead(event_type) over (partition by user_id, show_id order by "timestamp" asc) as next_event_type
    , row_number() over (partition by user_id, show_id order by "timestamp" asc)     as rn
  from saves_unsaves

)

, build_range as (

  select
    *
    , case
      when event_type = 'save'
        then lead("timestamp") over (partition by user_id, show_id order by rn)
    end as unsave_at
  from save_unsave_sequence
  -- only take the first event if there are two saves or unsaves in a row
  where event_type != previous_event_type or previous_event_type is null

)

select
  user_id
  , show_id
  , "timestamp" as save_at
  , unsave_at
from build_range
where event_type = 'save'

