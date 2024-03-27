with

invoice_items as (

  select
    id             as invoice_line_item_id
    , amount       as line_item_amount_cents
    , batch_timestamp
    , currency
    , description
    , discountable as is_discountable
    , invoice_id
    , merchant_id
    , period_end   as period_end_at
    , period_start as period_start_at
    --EM 11/02/2023: can we write a test that checks for the number of line items per invoice? The max shoudl be 8 (1 base + 3 packages) * 2, for the case when all 4 were prorated
    , plan_id
    , price_id
    , proration
    , quantity
    , source_id
    , subscription
  from {{ source('stripe_prod', 'invoice_line_items') }}

)

select * from invoice_items