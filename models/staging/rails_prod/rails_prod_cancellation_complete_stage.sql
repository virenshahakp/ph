with 

cancels as (

  select * from {{ ref('rails_prod_cancellation_complete_source') }}

)

select * from cancels
