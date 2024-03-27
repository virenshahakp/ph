with 

lapsed as (

  select * from {{ ref('rails_prod_trial_lapsed_source') }}

)

select * from lapsed
