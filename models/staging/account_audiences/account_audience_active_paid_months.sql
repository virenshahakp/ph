with

account_payments as (

  select * from {{ ref('fct_account_payments') }}

)

, paid_subscribers as (

  select *
  from {{ ref('fct_paid_user_subscription_range') }}
  where is_active is true

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
    paid_subscribers.account_id
    , 'paid-{{ payment }}{% if loop.last == true %}{% endif %}-mos-active'     as audience
    , 'Paid {{ payment }}{% if loop.last == true %}{% endif %} Months, Active' as audience_name
  from paid_subscribers
  join payments
    on (
      paid_subscribers.account_id = payments.account_id
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