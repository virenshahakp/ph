{% set bby_launch_date        =  '2020-09-24' %}
{% set bby_test_store_codes   = ('test', 'test') %}
{% set bby_online_store_codes = ('960', '2311') %}

with

events as (

  select
    activated.bby_serial_number                                            as serial_number
    , activated.bby_store_number                                           as store_number
    , activated.bby_sku                                                    as initial_sku
    , activated.bby_price_type                                             as price_code
    , case
      when activated.bby_price_type = '0'
        then 'paid something'
      when activated.bby_price_type = '1'
        then 'paid nothing'
      else 'unknown'
    end                                                                    as first_month_payment_type
    , activated.anonymous_id                                               as activation_code
    , case
      when activated.bby_store_number in {{ bby_online_store_codes }}
        then 'online'
      else 'in-store'
    end                                                                    as purchase_type
    , case
      when modified.bby_sku != ''
        then modified.bby_sku
      else activated.bby_sku
    end                                                                    as current_sku
    , identified.account_id                                                as account_id
    , case
      when redeemed.bby_serial_number is not null
        then started.received_at is not null
    end                                                                    as new_user
    , coalesce(airbyte.offer, 'unknown')                                   as bestbuy_offer_code
    , coalesce(started.subscriber_billing, reactivated.subscriber_billing) as biller
    , coalesce(started.subscriber_state, reactivated.subscriber_state)     as state
    , max(coalesce(started.received_at, reactivated.received_at))          as subscribed_at
    , max(activated.received_at)                                           as purchased_at
    , max(redeemed.received_at)                                            as redeemed_at
    , max(modified.received_at)                                            as last_modified_at
    , max(cancelled.received_at)                                           as cancelled_at
  -- "Activation" for BBY really means a confirmed purchase, and should drive all down stream events
  from {{ ref('bby_billing_api_prod_activated_stage') }} as activated
  left join
    {{ ref('bby_billing_api_prod_redeemed_stage') }} as redeemed
    on activated.bby_serial_number = redeemed.bby_serial_number
  left join
    {{ ref('bby_billing_api_prod_modified_stage') }} as modified
    on activated.bby_serial_number = modified.bby_serial_number
  left join
    {{ ref('bby_billing_api_prod_cancelled_stage') }} as cancelled
    on activated.bby_serial_number = cancelled.bby_serial_number
  left join
    {{ ref('airbyte_bestbuy_coupons_redeemed_source') }} as airbyte
    on activated.bby_serial_number = airbyte.bby_serial_number and airbyte.bby_transaction_date <= current_date
  left join {{ ref('bby_billing_api_prod_identifies_stage') }} as identified on redeemed.anonymous_id = identified.anonymous_id
  left join
    {{ ref('rails_prod_subscription_started_stage') }} as started
    on (identified.account_id = started.account_id and started.subscriber_billing = 'bestbuy')
  left join
    {{ ref('rails_prod_subscription_reactivated_stage') }} as reactivated
    on (identified.account_id = reactivated.account_id and reactivated.subscriber_billing = 'bestbuy')
  where activated.received_at >= '{{ bby_launch_date }}'
    and store_number not in {{ bby_test_store_codes }}
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13

)

select *
from events
