{{
    config(
      re_data_monitored=false
    )
}}

select
  account_id
  , received_at
  , refund_number
from {{ ref('rails_prod_numbered_refunds_stage') }}
where refund_number = 1
