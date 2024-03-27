with

charges as (

  select
    id                as charge_id
    , customer_id
    , batch_timestamp
    , amount          as amount_cents
    , amount_refunded as amount_refunded_cents
    , captured        as was_captured
    , captured_at
    , card_address_zip
    , card_address_zip_check
    , card_brand
    , card_country
    , card_customer_id
    , card_cvc_check
    , card_dynamic_last4
    , card_exp_month
    , card_exp_year
    , card_fingerprint
    , card_funding
    , card_id
    , card_iin
    , card_last4
    , card_name
    , card_network
    , created         as charge_created_at
    , dispute_id
    , failure_code
    , failure_message
    , invoice_id
    , outcome_network_advice_code
    , outcome_network_decline_code
    , outcome_network_status
    , outcome_reason
    , outcome_risk_level
    , outcome_risk_score
    , outcome_rule_id
    , outcome_seller_message
    , outcome_type
    , paid            as has_paid
    , payment_method_id
    , payment_method_type
    , receipt_number
    , refunded        as was_refunded
    , status          as charge_status
  from {{ source('stripe_prod', 'charges') }}

)

select * from charges