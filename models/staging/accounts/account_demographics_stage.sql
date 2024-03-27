with

household_summary as (

  select
    user_id
    , males
    , females
  from {{ ref('neustar_household_summary_stage') }}

)

, active_accounts as (

  select distinct account_id
  from {{ ref('fct_paid_user_subscription_range') }}
  where is_active is true

)

, users as (

  select *
  from {{ ref('rails_prod_users_stage') }}
  where is_account_owner is true

)

, households as (

  select * from {{ ref('neustar_households_stage') }}

)

, persons as (

  select *
  from {{ ref('neustar_persons_stage') }}
  where
    index = 1

)

, max_date_range as (

  select
    account_id
    , max(date_range_end_at) as end_date
  from {{ ref('fct_paid_user_subscription_range') }}
  group by 1

)

-- disabling alias alignment rule as it fails to parse the macros correctly in this file
-- noqa: disable=LT01
select
  users.*
  , persons.age_range                                                  as demographic_age_range
  , households.people_in_household                                     as demographic_people_in_household
  , case
    when household_summary.males > 0
      and household_summary.females = 0
      then 'Male'
    when household_summary.females > 0
      and household_summary.males = 0
      then 'Female'
    when household_summary.males > 0
      and household_summary.females > 0
      then 'Mixed Household'
  end                                                                  as demographic_gender
  , {{ neustar_1418_homeowner('households.homeowner') }}               as demographic_homeowner
  , {{ neustar_1418_household_composition('households.composition') }} as demographic_household_composition
  , {{ neustar_1418_education('persons.education') }}                  as demographic_education_level
  , 1000 * nullif(households.income_narrow, '')::integer               as demographic_income
  , case
    when users.roles != '[]'
      then 'Special Role'
    else
      case
        when active_accounts.account_id is not null
          or users.created_at > dateadd(day, -7, current_date)
          -- bby promotional accounts not included
          then 'Current'
        else 'Defunct'
      end
      || case
        when max_date_range.account_id is null
          then 'Trial'
        else 'Paid'
      end
      || case
        when users.account_id = users.user_id
          then ''
        else 'Profile'
      end
  end                                                                  as user_state
  , {{ is_billable('users.roles') }}                                   as is_billable
from users
left join households on (users.user_id = households.user_id)
left join household_summary on (users.user_id = household_summary.user_id)
left join active_accounts on (users.account_id = active_accounts.account_id)
left join persons on (users.user_id = persons.user_id)
left join max_date_range on (users.account_id = max_date_range.account_id)