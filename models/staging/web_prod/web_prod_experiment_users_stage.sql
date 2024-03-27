with

raw_data as (

  select
    anonymous_id      as anonymous_id
    , user_id         as user_id
    , experiment_name as experiment_name
    , variant         as variant
    , context         as context
    , event_timestamp as event_timestamp
    , loaded_at       as loaded_at
  from {{ ref('web_prod_experiment_viewed_source') }}

)

, xp_user_data as (

  select
    anonymous_id
    , user_id
    , experiment_name
    , variant
    , context
    , event_timestamp
    , loaded_at
  from raw_data
  where user_id is not null
    and experiment_name is not null
    and variant is not null

)

select * from xp_user_data
