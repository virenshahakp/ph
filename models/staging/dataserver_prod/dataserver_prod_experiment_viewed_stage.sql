with

experiment_viewed as (

  select
    user_id           as user_id
    , experiment_name as experiment_name
    , variant         as variant
    , event_timestamp as event_timestamp
    , loaded_at       as loaded_at
  from {{ ref('dataserver_prod_experiment_viewed_source') }}
)

select * from experiment_viewed
