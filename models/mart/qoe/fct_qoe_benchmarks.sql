{{ config(
    materialized = 'table'
    , sort = ['metric', 'metric_date', 'asset_type']
    , dist = 'ALL'
) }}

WITH

benchmarks AS (

  SELECT * FROM {{ ref('airbyte_quality_of_experience_benchmarks_stage') }}

)

SELECT
  asset_type
  , metric
  , metric_date
  , metric_period
  , platform
  , source
  , metric_value
FROM benchmarks