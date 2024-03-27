with 

ltv as (

  select distinct
    account_id
    , first_payment_at
    , ltv_revenue
    , avg_ltv_revenue
    , ltv_margin
  from {{ ref('fct_lifetime_value') }}

)

, users as ( 

  select
    user_id
    , account_id
    , created_at
  from {{ ref('dim_users') }} 

)

, watched as (

  select
    user_id
    , timestamp_start as watched_day
    , sum(minutes) as minutes
  from {{ ref('fct_watched_minutes') }} 
  {{ dbt_utils.group_by(n=2) }}

)

, visits as ( 

  select
    account_id
    , signed_up_at
    , first_paid_at
    , context_campaign_source
    , context_campaign_name
    , context_campaign_term
  from {{ ref('fct_acquisition_funnel') }} 

)

, watched_10 as (  

  select
    users.account_id
    , count(distinct date_trunc('day', watched.watched_day)) as active_days
  from watched
  join users
    on watched.user_id = users.account_id
  where watched.minutes > 10
  {{ dbt_utils.group_by(n=1) }}

)

select 
  visits.account_id
  , watched_10.active_days
  , ltv.ltv_revenue
  , coalesce(visits.context_campaign_source, 'organic') as context_campaign_source
  , coalesce(visits.context_campaign_name, 'organic') as context_campaign_name
  , coalesce(visits.context_campaign_term, 'organic' ) as context_campaign_term
  , date_trunc('day', visits.signed_up_at) as signed_up_at
  , date_trunc('day', visits.first_paid_at) as first_paid_at
  , max(distinct date_trunc('day', watched.watched_day)) as last_watched_date
from
  visits
join watched_10 on
    watched_10.account_id = visits.account_id
left join watched on
    watched.user_id = watched_10.account_id
left join ltv on
    ltv.account_id = watched_10.account_id
{{ dbt_utils.group_by(n=8) }}