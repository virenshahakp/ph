with

media_group as (

  select * from {{ ref('airbyte_media_groups_source') }}

)

select * from media_group
