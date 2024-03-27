{{ config(
    materialized = 'table'
    , sort = 'ad_month'
) }}



{{ dbt_utils.unpivot(
  relation=ref('airbyte_ad_revenue_by_network_stage'),
  cast_to='decimal',
  exclude=['ad_month'],
  remove=[],
  field_name='partner',
  value_name='revenue'
) }}
