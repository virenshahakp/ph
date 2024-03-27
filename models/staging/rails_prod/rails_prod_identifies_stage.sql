with

identifies as (

  select * from {{ ref('rails_prod_identifies_source') }}

)

, generate_valid_until as (

  select
    identifies.*
    , lead(received_at) over (partition by user_id order by received_at asc) as next_timestamp
  from identifies

)

select
  user_id
  , account_id
  , anonymous_id
  , received_at
  , hashed_session_id
  , subscriber_state
  , subscriber_billing
  , roles                             as user_roles
  , packages
  , coalesce(next_timestamp, sysdate) as end_at
from generate_valid_until
