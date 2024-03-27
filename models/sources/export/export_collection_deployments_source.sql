with

collection_deployments as (

  select
    id                                  as deployment_id
    , channel_id                        as channel_id
    , collection_version_id             as collection_version_id
    , experiment_id                     as experiment_id
    , variant                           as variant
    , position::int                     as collection_position_relative
    , environment                       as deployment_environment
    , is_active                         as is_active
    , lower(display_name)::varchar(128) as collection_display_name
    , coalesce(target, 'none')          as collection_target
  from {{ source('export','collection_deployments') }}

)

select * from collection_deployments