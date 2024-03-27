with

invoices as (

  select
    id                 as invoice_id
    , amount_due       as amount_due_cents
    , amount_paid      as amount_paid_cents
    , amount_remaining as amount_remaining_cents
    , application_fee
    , attempt_count
    , attempted        as has_attempted
    , auto_advance     as does_auto_advance
    , batch_timestamp
    , billing_reason
    , charge_id
    , collection_method
    , currency
    , customer_id
    , date             as invoice_created_at
    , discount_coupon_id
    , discount_customer_id
    , discount_end
    , discount_start
    , discount_subscription
    , due_date
    , effective_at
    , ending_balance
    , merchant_id
    , next_payment_attempt
    , number           as invoice_number
    , on_behalf_of_id
    , paid_out_of_band as was_paid_out_of_band
    , paid             as was_paid
    , period_end       as period_end_at --Thane: could be renamed due_at
    , period_start     as period_start_at
    , receipt_number
    , starting_balance
    , statement_descriptor
    , status_transitions_finalized_at
    , status_transitions_marked_uncollectible_at
    , status_transitions_paid_at  -- Thane: could be renamed paid_at 
    , status_transitions_voided_at
    , status           as invoice_status
    , subscription_id
    , subscription_proration_date
    , subtotal         as subtotal_cents
    , tax_percent
    , tax              as tax_cents
    , total            as total_cents
    {# TODO: DEV-16564
    , coalesce(tax_percent, 0) as tax_percent 
    , coalesce(tax, 0) as tax_cents  #}
    , transfer_data_amount
    , transfer_data_destination_id
    , webhooks_delivered_at
  from {{ source('stripe_prod', 'invoices') }}

)

select * from invoices