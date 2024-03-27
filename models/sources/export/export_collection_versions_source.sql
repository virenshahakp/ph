with

collection_versions as (

  select
    id               as collection_version_id
    , collection_id  as collection_id
    , version_number as collection_version_number
  from {{ source('export','collection_versions') }}

)

select * from collection_versions