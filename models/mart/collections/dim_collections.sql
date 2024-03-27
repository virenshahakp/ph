{{ 
  
  config(
    materialized='table'
    , sort='collection_id'
    , dist='collection_id' 
  ) 

}}

with

tile_group_ids as (

  select * from {{ ref('export_tile_group_ids_stage') }}

)

, collection_deployments as (

  select * from {{ ref('export_collection_deployments_stage') }}

)

, collection_versions as (

  select * from {{ ref('export_collection_versions_stage') }}

)

, collection_deployments_product_rules as (

  select * from {{ ref('export_collection_deployments_product_rules_stage') }}

)

, experiments as (

  select * from {{ ref('export_experiments_stage') }}

)

, collections as (

  select
    tile_group_ids.collection_id
    , tile_group_ids.collection_name
    , tile_group_ids.show_id
    , collection_deployments.collection_display_name
    , collection_deployments.channel_id
    , collection_deployments.collection_target
    , collection_deployments.deployment_id
    , collection_deployments.experiment_id
    , collection_deployments.deployment_environment
    , collection_deployments.collection_position_relative
    , collection_deployments.variant
    , collection_versions.collection_version_id
    , collection_versions.collection_version_number
    , collection_deployments_product_rules.product_rule_id
    , collection_deployments_product_rules.collection_product_rule
    , experiments.experiment_name
    , collection_deployments.deployment_id is not null as is_dynamic
  from tile_group_ids
  left join collection_deployments
    on
      tile_group_ids.deployment_id = collection_deployments.deployment_id
  left join collection_versions
    on
      collection_deployments.collection_version_id = collection_versions.collection_version_id
  left join experiments
    on
      experiments.experiment_id = collection_deployments.experiment_id
  left join collection_deployments_product_rules
    on
      collection_deployments.deployment_id = collection_deployments_product_rules.deployment_id

)

select * from collections