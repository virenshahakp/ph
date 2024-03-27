{{
  config(
    materialized='incremental'
    , dist='playback_session_id'
    , sort='dbt_processed_at'
    , on_schema_change = 'append_new_columns'
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

stream_error as (

  select
    {{ qoe_columns_select(skip_columns=[
        'device_name'
        , 'device_manufacturer'
        , 'device_model'
        , 'os_version'
        , 'position'
        , 'position_ms'
      ])
    }}
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
  from {{ ref('viziotv_prod_stream_error_source') }}

)

select *
from stream_error
{% if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{% endif %}
