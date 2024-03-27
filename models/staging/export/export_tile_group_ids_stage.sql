with

source as (

  select
    collection_name
    , deployment_id
    , show_id
    , channel_id
    , {{ normalize_id("collection_id") }} as collection_id
  from {{ ref('export_tile_group_ids_source') }}

)

select * from source