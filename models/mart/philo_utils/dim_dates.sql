{{ 
  config(
    materialized='incremental'
    , unique_key='observation_date'
    , sort='observation_date'
    , dist='ALL'
  )
}}
-- Use ALL distribution to place this table on all Redshift nodes so that joins
-- do not need to traverse the network.

{%- set max_date_at = incremental_max_value('observation_date') %}

with

dates as (

  select *
  from {{ ref('date_lookup') }}
  {% if is_incremental() %}
    where observation_date > {{ max_date_at }}
  {% endif %}

)

select * from dates