{{
  config(
    materialized='incremental'
    , unique_key='record_id'
    , dist='auto'
    , sort=['day_of', 'package', 'dma_sort']
  )
}}

with 

{%- set max_day = incremental_max_value('day_of') %}

date_spine as (

  select observation_date as day_of
  from {{ ref('dim_dates') }}
  where
    -- 2021-08-01 is the first month with complete data for the package_payment_succeeded start and end dates populated
    observation_date between '2021-08-01' and current_date
    {% if is_incremental() %}
      and (
        observation_date >= {{ max_day }}
        -- reprocess recent days in case stripe invoice statuses have updated to paid for some users
        or observation_date >= current_date - interval '45 days'
      )
    {% endif %}

)

, stripe_invoices as (

  select * from {{ ref('stripe_prod_invoices_stage') }}

)

, stripe_line_items as (

  select * from {{ ref('stripe_prod_invoice_line_items_stage') }}

)

, stripe_plans as (

  select * from {{ ref('stripe_prod_plans_stage') }}

)

, stripe_products as (

  select * from {{ ref('stripe_prod_products_stage') }}

)

, stripe_customers as (

  select * from {{ ref('stripe_prod_customers_stage') }}

)

, dmas as (

  select
    code                                    as dma_code
    , name                                  as dma_name
    , row_number() over (order by name asc) as dma_sort
  from {{ ref('dma_code_name') }}

)

, chargebee_addon_regular_payments as (

  select
    amount_cents
    , access_end
    , access_start
    , account_id
    , package
    , 'chargebee-regular' as biller
  from {{ ref('fct_package_payment_succeeded') }}
  -- packages paid through chargebee are only epix, starz, movie if they have additional characters
  -- e.g. 'starz-standard' that indicates they were paid on another biller
  where package in ('epix', 'starz', 'movie')
    -- regular schedule pre-payments have an access start and end
    and access_start is not null and access_end is not null

)

, chargebee_addon_prorated_payments as (

  select
    proration_cents        as amount_cents
    , proration_end        as access_end
    , proration_start      as access_start
    , account_id
    , package
    , 'chargebee-prorated' as biller
  from {{ ref('fct_package_payment_succeeded') }}
  -- packages paid through chargebee are only epix, starz, movie if they have additional characters
  -- e.g. 'starz-standard' that indicates they were paid on another biller
  where package in ('epix', 'starz', 'movie')
    -- prorated payments have a proration start and end 
    -- and are not the exact same range as the regular access (some events were recorded this way, and could generate double counting)
    and proration_end is not null and proration_start is not null
    and (proration_end != access_end and proration_start != access_start)

)

, stripe_addon_payments as (

  select
    stripe_line_items.line_item_amount_cents as amount_cents
    , stripe_line_items.period_end_at        as access_end
    , stripe_line_items.period_start_at      as access_start
    , stripe_customers.philo_user_id         as account_id
    , 'stripe'                               as biller
    , case
      when stripe_products.product_name = 'MGM+' then 'epix'
      when stripe_products.product_name = 'Starz' then 'starz'
      when stripe_products.product_name = 'Movies & More' then 'movie'
    end                                      as package
  from stripe_invoices
  join stripe_line_items on stripe_invoices.invoice_id = stripe_line_items.invoice_id
  join stripe_plans on stripe_line_items.plan_id = stripe_plans.plan_id
  join stripe_products on stripe_plans.product_id = stripe_products.product_id
  join stripe_customers on stripe_invoices.customer_id = stripe_customers.customer_id
  where stripe_invoices.invoice_status = 'paid'
    and stripe_products.product_name in ('MGM+', 'Starz', 'Movies & More')

)

, all_payments as (

  select
    amount_cents
    , access_end
    , access_start
    , account_id
    , biller
    , package
  from chargebee_addon_regular_payments
  union all
  select
    amount_cents
    , access_end
    , access_start
    , account_id
    , biller
    , package
  from chargebee_addon_prorated_payments
  union all
  select
    amount_cents
    , access_end
    , access_start
    , account_id
    , biller
    , package
  from stripe_addon_payments

)

, daily_totals as (

  select
    date_spine.day_of
    , all_payments.package
    , coalesce(dmas.dma_sort, 999)                                                              as dma_sort
    , coalesce(dmas.dma_code, 0)                                                                as dma_code
    , coalesce(dmas.dma_name, 'unknown')                                                        as dma_name
    , count(distinct all_payments.account_id)                                                   as active_paid_total
    , count(
      distinct case when all_payments.biller = 'chargebee-regular' then all_payments.account_id end
    )                                                                                           as active_paid_chargebee_regular_total
    , count(
      distinct case when all_payments.biller = 'chargebee-prorated' then all_payments.account_id end
    )                                                                                           as active_paid_chargebee_prorated_total
    , count(distinct case when all_payments.biller = 'stripe' then all_payments.account_id end) as active_paid_stripe_total
  from all_payments
  join date_spine on (date_spine.day_of between all_payments.access_start and all_payments.access_end)
  left join analytics.dim_accounts on all_payments.account_id = dim_accounts.account_id
  left join dmas on (dim_accounts.dma_code = dmas.dma_code)
  where dim_accounts.is_billable is not false -- null or true
  {{ dbt_utils.group_by(n=5) }}
  order by 1 asc, 2 asc, 3 asc

)

select
  day_of
  , package
  , dma_sort
  , dma_code
  , dma_name
  , active_paid_total
  , active_paid_chargebee_regular_total
  , active_paid_chargebee_prorated_total
  , active_paid_stripe_total
  , {{ dbt_utils.generate_surrogate_key(['day_of', 'package', 'dma_code']) }} as record_id
from daily_totals