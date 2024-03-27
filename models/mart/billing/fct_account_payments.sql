{{ config(materialized="table", dist="account_id", sort="received_at") }}
WITH 

all_payments AS (

  {{ dbt_utils.union_relations(
     relations=[
         ref('inferred_biller_payments_stage')
       , ref('confirmed_biller_payments_stage')
     ],
     include=[
         "account_id"
       , "received_at"
       , "packages"
       , "subscriber_state"
       , "subscriber_billing"
       , "amount"
       , "list_price"
       , "is_gift"
       , "promotion"
       , "rev_share_partner"
     ]
    ) 
  }}

)

SELECT 
  account_id
  , packages
  , subscriber_state
  , subscriber_billing
  , amount
  , list_price
  , is_gift
  , promotion
  , rev_share_partner
  , received_at
  , ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY received_at) AS lifetime_payment_number
FROM all_payments