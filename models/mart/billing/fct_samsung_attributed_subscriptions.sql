{{
  config(
    materialized = 'table',
    dist='account_id',
    sort='first_paid_at',
    tags=['daily']
    )
}}

/*

This model determines which first payments are on accounts that we attribute to samsung
for signup. Thus the subscription range that starts with this payment and any payments
from that point until the subscription range ends are part of a revenue share with samsung.

- account created in any time period (`rails_prod.account_created` )
- find all anonymous_ids related to the account (`fct_user_identifies` )
- find any of those anonymous_ids as having visited samsung.pages.path '/sign-up' prior to account creation
  these are the accounts that are possible to give samsung attribution for as the user encountered philo's app
  on their tv before they created their account
- A) check that '/go/activate' was visited (can we confirm signup code used?) after the visit to the `/sign-up` page
  B) if they use the QR code this step will be logged as a visit to /login/subscribe
     with utm parameter device_type=samsungtv
- check if account pays (excluding first pay refunds)
- check for add-ons added within 72 hours of (paid) subscription starting as that revenue is also shared

*/

with

accounts as (

  -- account creation time is a helpful attribute, but it isn't part of our business logic
  select
    user_id
    , signup_source
    , event_timestamp as created_at
  from {{ ref('rails_prod_account_created_stage') }}
  /*
    qa records
    -- where user_id IN (
    -- '90452be7-d955-452d-a053-02e2b9474ed2',
    -- 'a7150628-1768-4871-86a4-d878c1a0eeed',
    -- 'e5d57137-4522-4be4-a7df-37a8e4b2ef84')
  */

)

, idents as (

  /*
  the first time that the account was identified on each platform
  we need each anonymous_id for joins, but first ident time is by user_id
  and is shared amongst the anonymous_ids
  */
  select distinct
    user_id
    , anonymous_id
    , platform
    , min("timestamp") over (partition by user_id, platform) as first_platform_ident_at
  from {{ ref('fct_user_identifies') }}

)

, samsung_ident as (

  /*
  the first samsung platform ident
  this is when the user completed the login flow
  for the first time on samsung
  */
  select
    user_id
    , first_platform_ident_at as first_samsung_ident
  from idents
  where platform = 'samsung'

)

, samsung_signup as (

  /*
  samsung sign-up page views
  this indicates the user saw a samsung link on their tv
  which is the only way in the signup flow they can get the URL or
  qr code that is required for attribution
  */
  select
    anonymous_id
    , user_id
    , visited_at as "timestamp"
  from {{ ref('samsung_prod_pages_stage') }}
  -- where anonymous_id = '59ae3126-33a7-452f-a5fb-2e251dadd13c' -- QA record
  where context_page_path = '/sign-up'
  order by "timestamp"

)

, activated as (

  /*
  a visit to the web prod destination from the samsung vanity url (/go/activate)
  indicates the user followed the link from the tv.
  a visit to the web prod signup page with a url parameter of device_type=samsungtv
  indicates that the QR code link was used. We use regexp_count() as it outperforms
  a like clause in this scenario
  */
  select
    anonymous_id
    , user_id
    , visited_at as "timestamp"
  from {{ ref('web_prod_pages_stage') }}
  where (
    context_page_path = '/go/activate'
    and visited_at > '2022-10-01'
  ) or (
    context_page_path = '/login/signup'
    and regexp_count(url, 'device_type=samsungtv') > 0
    and visited_at > '2022-10-01'
  )

)

, pays as (

  /* first ever account payment */
  select
    account_id
    , received_at
    , amount
  from {{ ref('fct_account_payments') }}
  where lifetime_payment_number = 1

)

, paid_idents as (

  select
    accounts.created_at                 as account_created_at
    , accounts.signup_source
    , accounts.user_id                  as account_id
    , idents.anonymous_id               as anonymous_id
    , idents.first_platform_ident_at    as first_platform_ident_at
    , idents.platform                   as ident_platform
    , samsung_ident.first_samsung_ident as first_samsung_ident_at
    , pays.received_at                  as first_paid_at
    , min(samsung_signup."timestamp")   as samsung_signup_visit_at -- first visit is what matters
    , min(activated."timestamp")        as activated_visit_at -- first visit
  from accounts
  -- needs to have at least one samsung ident
  join samsung_ident on (accounts.user_id = samsung_ident.user_id)
  -- have to compare against other platforms and all of their anonymous_ids
  left join idents on (accounts.user_id = idents.user_id)
  -- had to visit signup page before creating the account, join idents because it has the anon_id
  left join
    samsung_signup
    on
      (idents.anonymous_id = samsung_signup.anonymous_id and accounts.created_at > samsung_signup."timestamp")
  -- had to visit the /go/activate page or the qr code page before creating the account 
  left join
    activated on (idents.anonymous_id = activated.anonymous_id and accounts.created_at > activated."timestamp")
  -- only accounts that pay will be relevant
  left join pays on (accounts.user_id = pays.account_id)
  where
    /*
    ident happened before samsung ident these are the possible attributable
    flows as you cannot create an account on samsung
    */
    first_platform_ident_at <= first_samsung_ident_at
    /*
    samsung didn't exist prior to 2022-10-01, so accounts created prior to this aren't attributable
    */
    and accounts.created_at > '2022-10-01'
    -- samsung login needs to have happened before they pay to have completed the attribution flow
    and samsung_ident.first_samsung_ident < pays.received_at
  {{ dbt_utils.group_by(n=8) }}
  order by account_id, first_platform_ident_at

)

, user_platform_summary as (

  select
    account_created_at
    , account_id
    , anonymous_id
    , first_platform_ident_at
    , ident_platform
    , first_samsung_ident_at
    , first_paid_at
    , min(samsung_signup_visit_at) over (partition by account_id)                  as samsung_signup_visit_at
    , min(activated_visit_at) over (partition by account_id)                       as activated_visit_at
    -- generate platform sequence for QA purposes
    , row_number() over (partition by account_id order by first_platform_ident_at) as seq
  from paid_idents

)

select *
from user_platform_summary
where first_paid_at is not null
