with 

reactivations as (
  select * from {{ ref('rails_prod_subscription_reactivated_source') }}
)

select *
from reactivations
