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
    -- Take the last error_code in the list as the error code
    , reverse(split_part(reverse(error_code_list), ',', 1)) as error_code
    , error_description                                     as raw_description
    , {{ qoe_apple_error_description('error_description') }}
    as error_description
    , error_philo_code
    , error_detailed_name
    , error_http_status_code
    , sysdate                                               as dbt_processed_at
  from {{ ref('tvos_prod_stream_error_source') }}

)

select *
from stream_error
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where loaded_at > {{ max_loaded_at }}

{% endif %}
