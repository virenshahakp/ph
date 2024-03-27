with 

blocked as (

  select * from {{ ref('rails_prod_trial_blocked_source') }}

)

select * from blocked
