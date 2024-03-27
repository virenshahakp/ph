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


, invoices as (

  select
    invoice_id
    , invoice_created_at
    , invoice_status
    , subtotal_cents
    , tax_percent
    , tax_cents
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
  from {{ ref('stripe_prod_invoices_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)
  -- TODO: we may want to limit this query based on the Stripe migration (completed 2023-02-24) 
  -- and last significant wave of charge events fired without invoices (2023-03-16)
)

, invoice_line_items as (

  select
    invoice_id
    , proration
    , plan_id
    , line_item_amount_cents
  from {{ ref('stripe_prod_invoice_line_items_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)

)

-- TODO: EM 12/04/23 stripe subscriptions table contains information about 
-- the trial which may be a more consolidated way to add info (also has tax_percent)
-- can test/compare later

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
  where invoice_id in (select invoice_id from invoices_to_process)
    and invoice_id is not null

)

, disputes as (

  select
    dispute_reason
    , dispute_status
    , dispute_amount_cents
    , batch_timestamp as disputes_batch_timestamp
    , charge_id
  from {{ ref('stripe_prod_disputes_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)

)


, refunds as (

  select
    refund_id
    , refund_reason
    , refund_status
    , refund_amount_cents
    , refund_created_at
    , batch_timestamp                                                             as refunds_batch_timestamp
    , charge_id
    , row_number() over (partition by invoice_id order by refund_created_at desc) as rn
  from {{ ref('stripe_prod_refunds_stage') }}
  where invoice_id in (select invoice_id from invoices_to_process)

)

, last_refund as (

  select
    refund_id             as last_refund_id
    , refund_reason       as last_refund_reason
    , refund_status       as last_refund_status
    , refund_amount_cents as last_refund_amount_cents
    , refund_created_at   as last_refund_created_at
    , refunds_batch_timestamp
    , charge_id
  from refunds
  where rn = 1

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

  select * from {{ ref('stripe_prod_plans_stage') }}

)

-- TODO: create some kind of dictionary (call it Y) with a list of the following product_ids:
-- base product_ids:
-- prod_LRt2BcmIJDMJlw	Philo Base
-- prod_LRt4dLKAiaFF0u	Philo Legacy
-- prod_LRt5OmpseDVzfM  Movies & More
-- prod_LRt5rJEfZjoiyB  Starz
-- prod_LRt5gws1nTR4ac  MGM+
-- OR establish new process of adding readable plan names to Stripe metadata and join here
-- ALSO: MGM+ price change coming 2024/02/08
-- AMC+ net new addon coming asap: two versions ad-free and ad-supported 

, line_items_summary as (

  select
    invoice_line_items.invoice_id
    , sum(case when invoice_line_items.proration = true then 1 end) as prorated_items_count
    -- EM 12/02/23: philo_base includes both legacy and base Philo subscriptions,
    -- since they are effectively the same product (starting when we migrated to Stripe) 
    -- the plan_id identifies the unique version/price a customer pays ($25/$20/$16)
    , max(
      case when stripe_plans.product_id = 'prod_LRt2BcmIJDMJlw' then invoice_line_items.plan_id
        when stripe_plans.product_id = 'prod_LRt4dLKAiaFF0u' then invoice_line_items.plan_id
      end
    )
    as philo_base
    -- the following stripe_plans.product_id is for the movies & more package 
    , max(
      case when stripe_plans.product_id = 'prod_LRt5OmpseDVzfM' then invoice_line_items.plan_id
      end
    )
    as movie_addon
    -- the following stripe_plans.product_id is for the starz package 
    , max(
      case when stripe_plans.product_id = 'prod_LRt5rJEfZjoiyB' then invoice_line_items.plan_id
      end
    )
    as starz_addon
    -- the following stripe_plans.product_id is for the MGM+ package 
    , max(
      case when stripe_plans.product_id = 'prod_LRt5gws1nTR4ac' then invoice_line_items.plan_id
      end
    )
    as mgm_addon
    , count(distinct stripe_plans.product_id)                       as invoice_product_count
    -- TODO: we may want to account for the shift from old proration to immediate proration (~June 2023)
    , sum(invoice_line_items.line_item_amount_cents)                as total_with_proration
    , count(1)                                                      as line_item_count
  from invoice_line_items
  left join stripe_plans
    on invoice_line_items.plan_id = stripe_plans.plan_id
  {{ dbt_utils.group_by(n=1) }}

)

, payment_method_switch as (

  select
    invoice_id
    , count(distinct coalesce(card_fingerprint, payment_method_id)) as payment_method_count
    -- the card_fingerprint is not provided for Link and Cashapp payments, so we can't differentiate 
    -- TODO: EM check that card_fingerprint, payment_method_id sufficient
  from charges
  {{ dbt_utils.group_by(n=1) }}

)


select
  --Invoices
  invoices.invoice_id
  , invoices.customer_id
  , invoices.charge_id
  , invoices.discount_coupon_id
  , invoices.subtotal_cents
  , invoices.tax_percent
  , invoices.tax_cents
  , invoices.total_cents
  , invoices.amount_paid_cents
  , invoices.was_paid
  , invoices.billing_reason
  , invoices.attempt_count
  , invoices.invoice_status
  , invoices.invoice_created_at
  -- Thane: status_transitions_paid_at aligns with Stripe API value for when the invoice was actually paid
  , invoices.period_end_at
  , invoices.status_transitions_paid_at
  , line_items_summary.invoice_product_count
  , line_items_summary.line_item_count
  , line_items_summary.prorated_items_count
  , line_items_summary.total_with_proration
  , line_items_summary.philo_base
  , line_items_summary.movie_addon
  , line_items_summary.mgm_addon
  , line_items_summary.starz_addon
  -- coming soon: amc_addon
  , customers.philo_user_id   as user_id
  , customers.zip             as customer_zip
  , customers.state           as customer_state
  , payment_method_switch.payment_method_count
  , charges.card_address_zip
  , charges.card_fingerprint --null for cashapp and link(stripe wallet)
  , charges.card_funding
  , charges.amount_refunded_cents
  , charges.was_refunded      as was_charge_refunded
  , charges.charge_status     as charge_status
  , charges.charge_created_at as charged_at
  , charges.amount_cents      as charge_amount_cents
  -- TODO: EM compare failure_ values or outcome_ values with Laura and Thane, discuss benefits of each
  , charges.failure_code
  , charges.failure_message
  , disputes.dispute_reason
  , disputes.dispute_status
  , disputes.dispute_amount_cents
  , charges.dispute_id
  , last_refund.last_refund_id
  , last_refund.last_refund_reason
  , last_refund.last_refund_status
  , last_refund.last_refund_amount_cents
  , last_refund.refunds_batch_timestamp
  , invoices.invoices_batch_timestamp
  , disputes.disputes_batch_timestamp
from invoices
left join charges on invoices.charge_id = charges.charge_id
left join line_items_summary on invoices.invoice_id = line_items_summary.invoice_id
left join customers on invoices.customer_id = customers.customer_id
left join last_refund on charges.charge_id = last_refund.charge_id
left join disputes on charges.charge_id = disputes.charge_id
left join payment_method_switch on invoices.invoice_id = payment_method_switch.invoice_id



