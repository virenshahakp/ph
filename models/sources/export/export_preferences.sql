with source as (

  select * from {{ source('export', 'preferences') }}

)

select
  id      as preference_id
  , user_id
  , name  as preference_name
  , value as preference_value
  , created_at
  , updated_at
from source
