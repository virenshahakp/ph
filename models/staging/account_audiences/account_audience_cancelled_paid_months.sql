with

account_payments as (

  select * from {{ ref('fct_account_payments') }}

)

, cancellation_complete as (

  select * from {{ ref('rails_prod_cancellation_complete_stage') }}

)

, paid_subscribers as (

  select * from {{ ref('fct_paid_user_subscription_range') }}

)

, cancelled_accounts as (

  -- Get the most recent cancellation time and state of users, removing any currently active users
  select
    cancellation_complete.account_id
    , last_value(cancellation_complete.received_at)
      over (
        partition by cancellation_complete.account_id
        order by cancellation_complete.received_at
        rows between unbounded preceding and unbounded following
      )
    as last_cancelled_at
  from cancellation_complete
  join paid_subscribers on (cancellation_complete.account_id = paid_subscribers.account_id)
  where
    cancellation_complete.account_id not in (
      select account_id
      from paid_subscribers
      where is_active is true
    )

)

, payments as (

  select
    account_id
    , max(lifetime_payment_number)                     as total_payments
    , count(distinct date_trunc('month', received_at)) as months_paid
  from account_payments
  group by 1

)

/*

  loop to produce multiple month cohorts
  this will produce cohorts of users who are actively at
  1, 2, 3, 4, or 5 months of payments -- not required to be consecutive
  and a final cohort that is 6+ months of payments

*/
{% for payment in range(1, 6) %}
  select
    cancelled_accounts.account_id
    , 'paid-{{ payment }}{% if loop.last == true %}
      
    {% endif %}-mos-cancelled'     as audience
    , 'Paid {{ payment }}{% if loop.last == true %}
      
    {% endif %} Months, Cancelled' as audience_name
  from cancelled_accounts
  join payments
    on (
      cancelled_accounts.account_id = payments.account_id
      {% if loop.last == true %}
        and payments.months_paid >= {{ payment }}
      {% else %}
        and payments.months_paid = {{ payment }}
      {% endif %}
    )

  {% if loop.last == false %}
    union all
  {% endif %}

{% endfor %}