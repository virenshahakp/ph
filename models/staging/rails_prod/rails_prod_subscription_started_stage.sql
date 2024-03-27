with 

subscriptions as (

  select * from {{ ref('rails_prod_subscription_started_source') }}

)

select *
  
from subscriptions
where 
  -- do not include 'bulk' OTT4EDU subscriptions
  bulk is null
  or bulk is false
