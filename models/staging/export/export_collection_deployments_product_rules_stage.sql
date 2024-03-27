with

source as (

  select
    product_rule_id
    , deployment_id
    , coalesce(
      trim(' ' from collection_product_rule), 'standard'
    ) as collection_product_rule
  from {{ ref('export_collection_deployments_product_rules_source') }}

)

select * from source