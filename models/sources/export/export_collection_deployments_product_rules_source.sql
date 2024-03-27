with

product_rules as (

  select
    id                         as product_rule_id
    , collection_deployment_id as deployment_id
    , product_rule_name        as collection_product_rule
  from {{ source('export','collection_deployments_product_rules') }}

)

select * from product_rules