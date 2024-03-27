with

signup_auth as (

  select * from {{ source('tvos_prod', 'signup_auth_submit') }}

)

select
  environment_analytics_version
  , "timestamp"
  , lower(anonymous_id)      as anonymous_id
  , lower(context_device_id) as context_device_id
from signup_auth
