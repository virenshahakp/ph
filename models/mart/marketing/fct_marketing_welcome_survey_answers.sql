{{ config(materialized = 'incremental', sort = 'received_at', tags=["daily", "exclude_hourly"]) }}

{%- set max_dbt_processed_at = incremental_max_value('dbt_processed_at') %}

with 

surveys as (

  select * from {{ ref('all_platforms_poll_submit_stage') }}

)

, survey_responses as (

  select * from surveys
  where poll_name = 'welcomeSurvey'
    {% if is_incremental() %}
      and dbt_processed_at > {{ max_dbt_processed_at }}
    {% endif %}

)

select * from survey_responses

