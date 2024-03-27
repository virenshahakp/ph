with

raw_data as (

  select
    anonymous_id      as anonymous_id
    , user_id         as user_id
    , experiment_name as experiment_name
    , variant         as variant
    , event_timestamp as event_timestamp
    , loaded_at       as loaded_at
  from {{ ref('rails_prod_experiment_viewed_source') }}
  /*
  LB: rails_prod had a bug where if the user did not meet the sample rate
  nil was set as the variant.  This is to exclude those users.
  */
  where variant is not null

)

select * from raw_data
