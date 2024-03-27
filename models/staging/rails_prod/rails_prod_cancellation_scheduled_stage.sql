with 

cancels as (

  select * from {{ ref('rails_prod_cancellation_scheduled_source') }}

)

select * from cancels
