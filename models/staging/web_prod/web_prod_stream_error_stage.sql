{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort='dbt_processed_at'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

stream_error as (

  select
    {{ qoe_columns_select(skip_columns=['position_ms']) }}
    , error_description
    , error_philo_code
    , error_detailed_name
    , error_http_status_code
    , case
      when raw_error_code = 'undefined'
        then '-1'
      else raw_error_code
    end                   as error_code
    , sysdate             as dbt_processed_at
  from {{ ref('web_prod_stream_error_source') }}

)

select *
from stream_error
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}
