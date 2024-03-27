{{
    config(
      re_data_monitored=false
    )
}}

select
  account_id
  , received_at
  , row_number() over (partition by account_id order by received_at) as refund_number
from {{ ref('rails_prod_payment_refunded_source') }}
order by 1, 2
