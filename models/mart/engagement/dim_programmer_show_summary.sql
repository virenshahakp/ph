{{ 
  config(
    materialized='incremental'
    , dist='dim_hash'
    , sort='channel_callsign'
    , unique_key='dim_hash'
  )
}}

{%- set max_processed_at = incremental_max_value('dbt_processed_at') %}

-- If any columns are added to this list, do a full refresh of the table
-- Casting needed on bool columns because Redshift cannot cast booleans to text (forced in surrogate key creation)
{%- set dim_columns = [
  'channel_callsign'
  , 'channel_name'
  , 'show_title'
  , 'dma_name'
  , 'demographic_gender'
  , 'demographic_age_range'
  , 'platform'
  , 'is_paid_programming::int' 
  , 'asset_type'
] %}

with

show_summary as (

  select *
  from {{ ref('fct_programmer_show_summary') }}
  {%- if is_incremental() %}
    where dbt_processed_at > {{ max_processed_at }}
  {%- endif %}

)

, show_summary_dim as (

  select
    {{ columns_select(dim_columns, skip_columns=['is_paid_programming::int']) }} 
    , is_paid_programming
    , max(dbt_processed_at) as dbt_processed_at
  from show_summary
  -- If any columns are added to this group by clause, do a full refresh of the table
  {{ dbt_utils.group_by(n=dim_columns|length) }}

)

, generate_dim_hash as (
  select
    *
    -- Must contain all of the columns in the group by clause above
    , {{ dbt_utils.generate_surrogate_key(dim_columns) }} as dim_hash
  from show_summary_dim
)

select * from generate_dim_hash