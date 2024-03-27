with

source as (

  select
    scrolled_id
    , uuid
    , user_id
    , received_at
    , event_timestamp
    , loaded_at
    , original_timestamp
    , tile_group_name
    , scrolled_event
    , event_text
    , scroll_distance
    , initial_index
    , terminal_index
    , environment_namespace
    , context_library_name
    , context_library_version
    , environment_analytics_version
    , f_base64encode(tile_group_id) as tile_group_id

  from {{ ref('dataserver_prod_scrolled_source') }}

)

select * from source









