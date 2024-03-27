{{
    config(
      re_data_time_filter='first_payment_failed_at'
    )
}}

-- migrated from BI dashboards, todo: adjust the aliasing for rule L027
-- noqa: disable=L027

with 

payments as (

  select * from {{ ref('rails_prod_payment_failed_source') }}

)

, first_payment as (

  select 
    account_id as account_id
    , min("timestamp") as first_payment_failed_at
  from
    payments
  group by 1

)

select 
  payments.*
  , first_payment_failed_at
from payments
left join first_payment on (payments.account_id = first_payment.account_id)
