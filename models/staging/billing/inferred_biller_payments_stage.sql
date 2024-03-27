{% set auto_renew_billers = ["bestbuy"] %}
with

paid_ranges as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, date_spine as (

  select day_of_philo as month_index
  from {{ ref('dim_dates') }}

)

, auto_renew_biller_ranges as (

  select
    paid_ranges.*
    , lag(subscriber_billing) over (partition by account_id order by date_range_start_at) as previous_subscriber_billing
    , lag(date_range_end_at) over (partition by account_id order by date_range_start_at)  as previous_range_end_at
  from paid_ranges
  where subscriber_billing in (
      {% for biller in auto_renew_billers %}
        '{{ biller }}'
        {% if not loop.last %},{% endif %}
      {% endfor %}

    )

)

, enumerate_continuous_ranges as (

  select
    *
    , sum(case
      -- no meaningful gap in paid access, don't consider a new range.
      when {{ dbt.datediff('previous_range_end_at::timestamp', 'date_range_start_at::timestamp', 'hours') }} between 0 and 1
        then 0 -- not a new range
      else 1 -- a new range
    end)
      over (partition by account_id order by date_range_start_at rows between unbounded preceding and current row)
    as paid_range_number
  from auto_renew_biller_ranges

)

, auto_renew_continuous_ranges as (

  select
    *
    , last_value(date_range_end_at) over (
      partition by account_id, paid_range_number
      order by date_range_start_at
      rows between unbounded preceding and unbounded following
    )                                                                                             as paid_access_end_at
    , row_number() over (partition by account_id, paid_range_number order by date_range_start_at) as rn
  from enumerate_continuous_ranges
  where 1 = 1
  qualify rn = 1

)

, range_duration as (

  select
    auto_renew_continuous_ranges.*
    , {{ 
        dbt.datediff(
          'auto_renew_continuous_ranges.date_range_start_at::timestamp'
          , 'auto_renew_continuous_ranges.paid_access_end_at::timestamp'
          , 'months'
        ) 
      }} + 1 as range_months
  from auto_renew_continuous_ranges

)

, range_sequence as (

  select
    range_duration.*
    , date_spine.month_index                                                     as range_payment_number
    , add_months(range_duration.date_range_start_at, date_spine.month_index - 1) as payment_date
  from range_duration
  join date_spine on (range_duration.range_months >= date_spine.month_index)

)

-- hard coding the $20.00 price point for best buy
, each_bill as (

  select
    account_id
    , packages
    , subscriber_state
    , subscriber_billing
    , 20.0::numeric(25, 4)                                              as list_price
    {# , auto_renew_amount                                                 as list_price #}
    , date_range_start_at
    , paid_access_end_at                                                as date_range_end_at
    , seq                                                               as paid_range_number
    , range_payment_number
    , payment_date
    , 20.0::numeric(25, 4)                                              as amount
    {# , case
      when range_payment_number = 1
        then first_bill_amount
      else auto_renew_amount
    end                                                                 as amount #}
    , row_number() over (partition by account_id order by payment_date) as payment_number
  from range_sequence
  where payment_date < paid_access_end_at

)

select
  account_id
  , payment_date as received_at
  , packages
  , subscriber_state
  , subscriber_billing
  , amount
  , list_price
  , false        as is_gift
  , null         as promotion
  , payment_number
from each_bill