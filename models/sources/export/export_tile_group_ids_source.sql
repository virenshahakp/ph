with

tile_group_ids as (

  select
    id                       as collection_id
    , deployment_id          as deployment_id
    , show_id                as show_id
    , channel_id             as channel_id
    , lower(collection_name) as collection_name
  from {{ source('export','tile_group_ids') }}

)

select * from tile_group_ids