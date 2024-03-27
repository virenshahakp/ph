with

dropped_packages as (

  select * from {{ source('rails_prod', 'package_dropped') }}

)

, renamed as (

  select
    id                as package_dropped_id
    , user_id         as account_id
    , package         as package
    , trial_duration  as trial_duration
    , trial_remaining as trial_remaining
    , event           as event
    , received_at     as received_at
  from dropped_packages

)

select * from renamed
