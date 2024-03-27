{{ config(
    materialized = 'table'
    , dist='ALL'
    , sort = 'ad_date'
) }}


WITH 

daily_spend AS (

  {{ 
    dbt_utils.unpivot(
    relation=ref('airbyte_daily_spend_stage'),
    cast_to='decimal',
    exclude=['ad_date'],
    remove=['total_spend'],
    field_name='partner',
    value_name='spend'
    ) 
  }}

)

SELECT 
  ad_date
  , partner
  , spend
FROM daily_spend
