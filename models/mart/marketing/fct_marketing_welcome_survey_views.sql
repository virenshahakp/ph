{{ 
  config(
    materialized='incremental',
    sort='received_at',
    tags=["daily", "exclude_hourly"]
  )
}}

{%- set max_dbt_processed_at = incremental_max_value('dbt_processed_at') %}

with 

surveys as (

  select 
    user_id
    , event_timestamp
    , answers as original_answers
    , dbt_processed_at
    , json_extract_array_element_text(answers, 0) as answers 
  from {{ ref('rails_prod_message_sent_stage') }}
  where message_channel = 'inApp'
    and message_name = 'welcomeSurvey'
    {% if is_incremental() %}
      and dbt_processed_at > {{ max_dbt_processed_at }} 
    {% endif %}

)

, numbers as (

  select day_of_philo as ordinal from {{ ref('dim_dates') }} where day_of_philo < 30

)

, joined as (

  select 
    surveys.user_id
    , surveys.event_timestamp
    , numbers.ordinal::int as option_number
    , surveys.dbt_processed_at
    , json_array_length(surveys.answers, true) as number_of_items
    , json_extract_array_element_text( 
      surveys.answers 
      , numbers.ordinal::int
      , true
    ) as item
  from surveys
  cross join numbers
  --only generate the number of records in the cross join that corresponds
  --to the number of items in the order
  where numbers.ordinal
    < json_array_length(surveys.answers, true)

)

, parsed as (

  --before returning the results, actually pull the relevant keys out of the
  --nested objects to present the data as a SQL-native table.
  --make sure to add types for all non-VARCHAR fields.
  select 
    user_id
    , event_timestamp
    , option_number
    , dbt_processed_at    
    , json_extract_path_text(item, 'id') as item_id 
    , json_extract_path_text(item, 'name') as item_name 
  from joined
    
)

select 
  user_id
  , event_timestamp as received_at
  , item_name
  , option_number
  , dbt_processed_at  
from parsed