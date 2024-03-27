{{ config(
    materialized = 'table'
    , dist='ALL'
    , sort = 'month'
) }}


SELECT
  month
  , quarter
  , aws_costs_per_user
  , taskus_costs_per_user
  , marketing_costs_total
  , customer_acquisition_costs_per_user
  , ad_revenue
  , content_costs
  , ad_costs
  , variable_costs
  , platform_costs 
  , fastly_edgecast
  , stripe_chargebee
  , gracenote
  , ending_subscribers
FROM {{ ref('airbyte_costs_stage') }}
