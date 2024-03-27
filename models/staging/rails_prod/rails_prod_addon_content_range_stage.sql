with

proration as (

  select * from {{ ref('rails_prod_addon_proration_range_stage') }}

)

, paid as (

  select * from {{ ref('rails_prod_addon_paid_range_stage') }}

)

, prorated_users_and_packages as (

  select
    account_id
    , package
  from proration

)

, paid_users_and_packages as (

  select
    account_id
    , package
  from paid

)

, all_users_and_packages as (

  select * from prorated_users_and_packages
  union distinct
  select * from paid_users_and_packages

)

, users_and_packages as (

  select distinct
    account_id
    , package
  from all_users_and_packages

)

select
  users_and_packages.account_id as account_id
  , users_and_packages.package  as package
  , proration.date_range_start  as proration_start
  , proration.date_range_end    as proration_end
  , proration.active            as proration_active
  , paid.date_range_start       as paid_start
  , paid.date_range_end         as paid_end
  , paid.active                 as paid_active
from users_and_packages
left join proration
  on (
    users_and_packages.account_id = proration.account_id
    and users_and_packages.package = proration.package
  )
left join paid
  on (
    users_and_packages.account_id = paid.account_id
    and users_and_packages.package = paid.package
  )
where coalesce(proration.package, paid.package) is not null
