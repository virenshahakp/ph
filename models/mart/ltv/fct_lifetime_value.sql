{{ config(materialized="table", dist="account_id", sort="account_id", tags=["daily", "exclude_hourly"]) }}
with

accounts as (

  select * from {{ ref('dim_accounts') }} where is_billable is true

)

, ltv_stage as (

  select * from {{ ref('ltv_stage') }}

)

-- noqa: disable=L016
-- disable line length checks
select
  ltv_stage.account_id
  , accounts.first_payment_at
  , count(distinct ltv_stage.month) as months_with_payments
  , sum(ltv_stage.margin)           as ltv_margin
  , sum(ltv_stage.costs)            as ltv_costs
  , sum(ltv_stage.revenue)          as ltv_revenue
  , avg(ltv_stage.revenue)          as avg_ltv_revenue

  -- 1st month value and costs
  , sum(
    case when ltv_stage.month = date_trunc('month', accounts.first_payment_at) then ltv_stage.margin else 0 end
  )                                 as ltv_margin_one_month
  , sum(
    case when ltv_stage.month = date_trunc('month', accounts.first_payment_at) then ltv_stage.costs else 0 end
  )                                 as ltv_costs_one_month
  , sum(
    case when ltv_stage.month = date_trunc('month', accounts.first_payment_at) then ltv_stage.revenue else 0 end
  )                                 as ltv_revenue_one_month
  -- no avg ltv revenue needed as it is the same as the one month ltv

  -- 1st three months value and costs, NULL if 3 months have not elapsed
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 3))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 3)) - interval '1 day' then ltv_stage.margin
          else 0
        end
      )
  end                               as ltv_margin_three_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 3))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 3)) - interval '1 day' then ltv_stage.costs
          else 0
        end
      )
  end                               as ltv_costs_three_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 3))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 3)) - interval '1 day' then ltv_stage.revenue
          else 0
        end
      )
  end                               as ltv_revenue_three_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 3))
      then null
    else
      avg(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 3)) - interval '1 day' then ltv_stage.revenue
          else 0
        end
      )
  end                               as avg_ltv_revenue_three_months

  -- 1st six months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 6))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 6)) - interval '1 day' then ltv_stage.margin
          else 0
        end
      )
  end                               as ltv_margin_six_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 6))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 6)) - interval '1 day' then ltv_stage.costs
          else 0
        end
      )
  end                               as ltv_costs_six_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 6))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 6)) - interval '1 day' then ltv_stage.revenue
          else 0
        end
      )
  end                               as ltv_revenue_six_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 6))
      then null
    else
      avg(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 6)) - interval '1 day' then ltv_stage.revenue
          else 0
        end
      )
  end                               as avg_ltv_revenue_six_months

  -- 1st year
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 12))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 12)) - interval '1 day' then ltv_stage.margin
          else 0
        end
      )
  end                               as ltv_margin_twelve_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 12))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 12)) - interval '1 day' then ltv_stage.costs
          else 0
        end
      )
  end                               as ltv_costs_twelve_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 12))
      then null
    else
      sum(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 12)) - interval '1 day' then ltv_stage.revenue
          else 0
        end
      )
  end                               as ltv_revenue_twelve_months
  , case
    when
      date_trunc('month', add_months(current_date, 1)) < date_trunc('month', add_months(accounts.first_payment_at, 12))
      then null
    else
      avg(
        case
          when
            ltv_stage.month between date_trunc(
              'month', accounts.first_payment_at
            ) and date_trunc('month', add_months(accounts.first_payment_at, 12)) - interval '1 day' then ltv_stage.revenue
          else 0
        end
      )
  end                               as avg_ltv_revenue_twelve_months

from ltv_stage
left join accounts on (ltv_stage.account_id = accounts.account_id)
{{ dbt_utils.group_by(n=2) }}