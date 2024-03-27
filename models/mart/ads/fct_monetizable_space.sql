{{ config(materialized='view'
, tags=["dai", "exclude_hourly", "exclude_daily"]
) }}

select
  *
  , {{ advertising_contract_types() }} as contract_type
from {{ ref('fct_ad_pods_enriched') }}
where
  {{ monetizable_space() }}