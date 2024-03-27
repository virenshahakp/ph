{{
    config(
      re_data_time_filter='first_payment_refunded_at'
    )
}}

-- migrated from BI dashboards, todo: adjust the aliasing for rule L027
-- noqa: disable=L027

with 

refunds as (

  select * from {{ ref('rails_prod_payment_refunded_source') }}

)

, first_refund as (

  select 
    account_id as account_id
    , min("timestamp") as first_payment_refunded_at
  from
    refunds
  group by 1

)

select 
  refunds.*
  , first_payment_refunded_at
from refunds
left join first_refund on (refunds.account_id = first_refund.account_id)
