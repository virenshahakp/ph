{{
  config(
    materialized='incremental'
    , unique_key='invoice_id'
    , dist='invoice_id'
    , sort=['invoice_created_at']
  )
}}


{%- set invoices_max_loaded_at = incremental_max_value('invoices_batch_timestamp') %}
{%- set disputes_max_loaded_at = incremental_max_value('disputes_batch_timestamp') %}
{%- set refunds_max_loaded_at = incremental_max_value('refunds_batch_timestamp') %}
--these product_ids correspond with Philo Base and Legacy stripe_plans 
{%- set base_plan_ids = 'prod_LRt2BcmIJDMJlw', 'prod_LRt4dLKAiaFF0u' %}

with

invoices_new_entries as (
  select invoice_id
  from {{ ref('stripe_prod_invoices_stage') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ invoices_max_loaded_at }}
  {% endif %}
)

, disputes_new_entries as (

  select invoice_id
  from {{ ref('stripe_prod_disputes_stage') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ disputes_max_loaded_at }}
  {% endif %}

)

, refunds_new_entries as (

  select invoice_id
  from {{ ref('stripe_prod_refunds_stage') }}
  {% if is_incremental() %}
    where batch_timestamp > {{ refunds_max_loaded_at }}
  {% endif %}

)

, invoices_to_process as (

  select invoice_id from invoices_new_entries
  union all
  select invoice_id from disputes_new_entries
  union all
  select invoice_id from refunds_new_entries


)

, invoice_line_items as (

  select
    invoice_line_item_id
    , invoice_id
    , price_id
    , plan_id
    , description
    , line_item_amount_cents as line_item_subtotal_cents
    , proration
    , is_discountable
    , batch_timestamp        as line_items_batch_timestamp
    /* TODO: DEV-16564
     period_end in line items is exactly one month after period_end in invoices
     the exceptions are cases when users had "remaining time" and appear to be partial billed for a previous month
    , period_end_at
    , period_start_at */
  from {{ ref('stripe_prod_invoice_line_items_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)

)

, invoices as (

  select
    invoice_id
    , invoice_created_at
    , invoice_status
    , subtotal_cents
    , total_cents
    , amount_paid_cents
    , attempt_count
    , discount_coupon_id
    , billing_reason
    , customer_id
    , was_paid
    , charge_id
    , period_end_at
    , status_transitions_paid_at
    , batch_timestamp as invoices_batch_timestamp
    -- TODO: DEV-16564
    , coalesce(
      tax_percent, 0
    )                 as tax_percent
    , coalesce(
      tax_cents, 0
    )                 as tax_cents
    , case
      when discount_coupon_id is not null then subtotal_cents - (total_cents - coalesce(tax_cents, 0)) else 0
    end               as discount_cents
  from {{ ref('stripe_prod_invoices_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)

)


, charges as (

  select
    charge_id
    , invoice_id
    , charge_created_at
    , charge_status
    , amount_cents
    , amount_refunded_cents
    , card_fingerprint
    , card_funding
    , card_address_zip
    , dispute_id
    , failure_code
    , failure_message
    , has_paid
    , payment_method_id
    , was_refunded
  from {{ ref('stripe_prod_charges_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process where invoice_id is not null)

)

, disputes as (

  select
    dispute_reason
    , dispute_status
    , dispute_amount_cents
    , dispute_created_at
    , batch_timestamp as disputes_batch_timestamp
    , charge_id
  from {{ ref('stripe_prod_disputes_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)

)


, last_refund as (

  select
    refund_id                                                                     as last_refund_id
    , refund_reason                                                               as last_refund_reason
    , refund_status                                                               as last_refund_status
    , refund_amount_cents                                                         as last_refund_amount_cents
    , refund_created_at                                                           as last_refund_created_at
    , batch_timestamp                                                             as refunds_batch_timestamp
    , charge_id
    , row_number() over (partition by invoice_id order by refund_created_at desc) as rn
  from {{ ref('stripe_prod_refunds_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)
  qualify rn = 1

)


, customers as (

  select
    philo_user_id
    , zip
    , state
    , customer_id
  from {{ ref('stripe_prod_customers_stage') }}

)

, stripe_plans as (

  select
    plan_id
    , product_id
    , nickname
    , plan_amount_cents
    , billing_interval
    , created_at
  from {{ ref('stripe_prod_plans_stage') }}

)

, discounted_line_items as (

  select
    invoice_line_items.invoice_line_item_id
    , invoice_line_items.invoice_id
    , invoice_line_items.price_id
    , invoice_line_items.plan_id
    , invoice_line_items.description
    , invoice_line_items.line_item_subtotal_cents
    , invoice_line_items.proration
    , invoice_line_items.is_discountable
    , invoice_line_items.line_items_batch_timestamp
    --the following logic matches discount information to base package line items, as discounts apply only to base plan 
    , case
      when stripe_plans.product_id in {{ base_plan_ids }} and invoice_line_items.proration is false
        then invoice_line_items.line_item_subtotal_cents - invoices.discount_cents
      else invoice_line_items.line_item_subtotal_cents
    end as line_item_discounted_subtotal_cents
    , case
      when stripe_plans.product_id in {{ base_plan_ids }} and invoice_line_items.proration is false
        then invoices.discount_coupon_id
    end as line_item_discount_coupon_id
    , case
      when stripe_plans.product_id in {{ base_plan_ids }} and invoice_line_items.proration is false
        then invoices.discount_cents
      else 0
    end as line_item_discount_cents
  from invoice_line_items
  left join invoices
    on invoice_line_items.invoice_id = invoices.invoice_id
  left join stripe_plans
    on invoice_line_items.plan_id = stripe_plans.plan_id
)


, line_item_totals as (

  select
    discounted_line_items.invoice_line_item_id
    , discounted_line_items.invoice_id
    , discounted_line_items.price_id
    , discounted_line_items.line_item_discount_coupon_id
    , discounted_line_items.plan_id
    , discounted_line_items.description
    -- discount_cents will be NULL for non-base package line items, 0 for non-discounted base packages
    , discounted_line_items.line_item_discount_cents
    , discounted_line_items.line_item_subtotal_cents
    , discounted_line_items.line_item_discounted_subtotal_cents
    , discounted_line_items.proration
    , discounted_line_items.is_discountable
    , discounted_line_items.line_items_batch_timestamp
    , round(
      (1 + (invoices.tax_percent / 100)) * discounted_line_items.line_item_discounted_subtotal_cents, 0
    )                                                           as line_item_total_cents
    , round((1 + (invoices.tax_percent / 100)) * discounted_line_items.line_item_discounted_subtotal_cents, 0)
    - discounted_line_items.line_item_discounted_subtotal_cents as line_item_tax_cents
  from discounted_line_items
  join invoices on discounted_line_items.invoice_id = invoices.invoice_id

)

select
  line_item_totals.invoice_line_item_id
  , line_item_totals.invoice_id
  , line_item_totals.price_id
  , line_item_totals.line_item_discount_coupon_id
  , line_item_totals.description
  , line_item_totals.line_item_subtotal_cents
  , line_item_totals.line_item_discount_cents
  , line_item_totals.line_item_discounted_subtotal_cents
  , line_item_totals.line_item_tax_cents
  , line_item_totals.line_item_total_cents
  , line_item_totals.proration
  , line_item_totals.line_items_batch_timestamp
  , invoices.total_cents                 as invoice_total_cents
  , invoices.customer_id
  , invoices.charge_id
  , invoices.invoice_status
  , invoices.attempt_count
  , invoices.billing_reason
  , invoices.was_paid
  , invoices.tax_percent
  , invoices.invoice_created_at
  , invoices.period_end_at
  , invoices.status_transitions_paid_at
  , invoices.invoices_batch_timestamp
  , charges.amount_cents                 as invoice_level_charge_cents
  , charges.payment_method_id
  , charges.dispute_id
  , charges.charge_created_at
  , charges.charge_status
  , charges.card_fingerprint
  , charges.card_funding
  , charges.card_address_zip
  , charges.failure_code
  , charges.failure_message
  , charges.has_paid
  , charges.was_refunded
  , disputes.dispute_amount_cents        as invoice_level_disputes_cents
  , disputes.dispute_reason
  , disputes.dispute_status
  , disputes.dispute_created_at
  , disputes.disputes_batch_timestamp
  , last_refund.last_refund_amount_cents as invoice_level_last_refund_amount_cents
  , last_refund.last_refund_id
  , last_refund.last_refund_reason
  , last_refund.last_refund_status
  , last_refund.last_refund_created_at
  , last_refund.refunds_batch_timestamp
  , customers.philo_user_id              as user_id
  , customers.zip                        as customer_zip
  , customers.state                      as customer_state
  , stripe_plans.nickname
  , stripe_plans.product_id
  , round(
    coalesce(charges.amount_refunded_cents * (line_item_totals.line_item_total_cents::float / invoices.total_cents::float), 0), 0
  )                                      as line_item_refund_cents

  , round(
    coalesce(disputes.dispute_amount_cents * (line_item_totals.line_item_total_cents::float / invoices.total_cents::float), 0), 0
  )                                      as line_item_dispute_cents
  , round(
    coalesce(
      last_refund.last_refund_amount_cents * (line_item_totals.line_item_total_cents::float / invoices.total_cents::float), 0
    )
    , 0
  )                                      as line_item_last_refund_cents
from line_item_totals
left join invoices on line_item_totals.invoice_id = invoices.invoice_id
left join charges on invoices.charge_id = charges.charge_id
left join customers on invoices.customer_id = customers.customer_id
left join last_refund on charges.charge_id = last_refund.charge_id
left join disputes on charges.charge_id = disputes.charge_id
left join stripe_plans on line_item_totals.plan_id = stripe_plans.plan_id