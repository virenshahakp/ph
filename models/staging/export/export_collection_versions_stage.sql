with

source as (

  select
    collection_version_id
    , collection_version_number
    , {{ normalize_id("collection_id") }} as collection_id
  from {{ ref('export_collection_versions_source') }}

)

select * from source