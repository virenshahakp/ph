{{ 
  config(
    materialized = 'table'
    , sort = ['impression_date', 'provider']
    , dist = 'AUTO'
  ) 
}}

-- recent (e.g. current year data), could be done incrementally, but these are small tables
select
  impression_date
  , provider
  , {{ try_cast_numeric(
      'impressions',
      'decimal',
      'FM9G999G999D99')
  }} as impressions
  , {{ try_cast_numeric(
        'revenue', 
        'decimal', 
        'FM9G999G999D99') 
  }} as revenue
from {{ ref('airbyte_ad_revenue_stage') }}

union 

-- fixed historic revenue data
select
  impression_date
  , provider
  , {{ try_cast_numeric(
      'impressions',
      'decimal',
      'FM9G999G999D99')
  }} as impressions  
  , {{ try_cast_numeric(
        'revenue', 
        'decimal', 
        'FM9G999G999D99') 
  }} as revenue
from {{ ref('uploads_ad_revenue_historic_stage') }}
