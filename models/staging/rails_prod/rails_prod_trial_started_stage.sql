with 

starts as (

  select * from {{ ref('rails_prod_trial_started_source') }}

)

select * from starts