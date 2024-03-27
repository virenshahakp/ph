with

source as (

  select
    deployment_id
    , channel_id
    , collection_version_id
    , experiment_id
    , collection_position_relative
    , collection_target
    , deployment_environment
    , collection_display_name
    , coalesce(variant, 'control') as variant
  from {{ ref('export_collection_deployments_source') }}

)

select * from source