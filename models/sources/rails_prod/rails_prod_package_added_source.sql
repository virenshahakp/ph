with

added_packages as (

  select * from {{ source('rails_prod', 'package_added') }}

)

, renamed as (

  select
    id                as package_added_id
    , user_id         as account_id
    , package         as package
    , trial_duration  as trial_duration
    , trial_remaining as trial_remaining
    , event           as event
    , received_at     as received_at
  from added_packages

)

select * from renamed
