with

rno as (

  select * from {{ source('periscope_views', 'rno_users_aug_10_2020') }}

)

select
  rno.*
  , rno.user_id as account_id
from rno