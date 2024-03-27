with

scrolled as (

  select * from {{ source('dataserver_prod', 'scrolled') }}

)

, renamed as (

  select

    id                                    as scrolled_id
    , uuid
    , user_id
    , received_at
    , "timestamp"                         as event_timestamp
    , uuid_ts                             as loaded_at
    , original_timestamp
    , tile_group                          as tile_group_name
    , event                               as scrolled_event
    , event_text
    , scroll_distance
    , initial_index
    , terminal_index
    , environment_namespace
    , context_library_name
    , context_library_version
    , environment_analytics_version
    /* steps used construct dataserver_prod_scrolled_stage.tile_group_id to match values in datasever_prod_scrolled_source.tile_group_id:
    1. Manually add the prefix 'TileGroup:'' to scrolled.tile_group_id to create datasever_prod_scrolled_source.tile_group_id
    2. encode datasever_prod_scrolled_source.tile_group_id to create datasever_prod_scrolled_stage.tile_group_id so that this field can match playback_session_created.tile_group_id
    */
    , concat('TileGroup:', tile_group_id) as tile_group_id

  from scrolled

)

select * from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
