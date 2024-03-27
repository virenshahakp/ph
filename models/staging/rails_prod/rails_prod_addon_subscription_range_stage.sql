-- migrated from BI dashboards, todo: adjust the aliasing/syntax for rules L016, L027, L031 & L035
-- noqa: disable=L027,L031,L016,L035

--
-- With the introduction of addons, each with an independent trial, and proration (used to cover
-- costs for content access granted until a user's next, regularly scheduled bill), the "typical"
-- sequence is to go from trial, to proration, to paid phases. But users can cancel at an point.
--
-- This view is intended to create "tenure" records describing a contiguous set of phases which
-- match one of the following sequences. Note that proration and paid phases will have to be
-- contiguous, as there is natural flow from one (pre-bill) to the other (post-bill). However,
-- there is no requirement that trials necessarily flow into proration, so trial ranges are treated
-- a bit differently than the content ranges (proration and paid) for which content costs are due.
--
-- NOTE: Case 5, 6, and 7 are just case 2, 3, and 4 with a contiguous, leading trial. This fact
--       is leveraged below in how content ranges (proration and paid) are joined to trials.
--
-- Case #1 Trial only
--   |--- trial ---|    (trial completed); phase: "churned"
--   |--- trial --->    (active trial);    phase: "trial"
--
--
-- Case #2 Trial and Proration
--   |--- trial ---|--- proration ---|    (trial completed, proration completed); phase: "churned"
--   |--- trial ---|--- proration --->    (trial completed, active proration);    phase: "proration"
--
--
-- Case #3 Trial, Proration, and Paid
--   |--- trial ---|--- proration ---|--- paid ---|    (trial completed, proration completed, paid completed); phase: "churned"
--   |--- trial ---|--- proration ---|--- paid --->    (trial completed, proration completed, active paid);    phase: "billed"
--
--
-- Case #4 Trial and Paid
--   |--- trial ---|--- paid ---|    (trial completed, paid completed); phase: "churned"
--   |--- trial ---|--- paid --->    (trial completed, active paid);    phase: "billed"
--
--
-- Case #5 Proration only
--   |--- proration ---|    (proration completed); phase: "churned"
--   |--- proration --->    (active proration);    phase: "proration"
--
--
-- Case #6 Proration and Paid
--   |--- proration ---|--- paid ---|    (proration completed, paid completed); phase: "churned"
--   |--- proration ---|--- paid --->    (proration completed, active paid);    phase: "billed"
--
--
-- Case #7 Paid only
--   |--- paid ---|    (paid completed); phase: "churned"
--   |--- paid --->    (active paid);    phase: "billed"
--

with

content_ranges as (

  select
    account_id
    , package
    , proration_start
    , proration_end
    , proration_active
    , paid_start
    , paid_end
    , paid_active
    , case

      -- case #5
      -- proration range only; may be active or completed
      when proration_end is not null and paid_end is null
        then 5

      -- case #6
      -- proration which is completed, and rolled into billed, may be active or completed
      when proration_end is not null and paid_end is not null
        and proration_end between paid_start and paid_end
        then 6

      -- case #7
      -- billed range only; may be active or completed
      when proration_end is null and paid_end is not null
        then 7
    end as billing_case
  from {{ ref('rails_prod_addon_content_range_stage') }}

)

, contiguous_ranges as (

  select
    content_ranges.account_id
    , content_ranges.package
    , proration_start
    , proration_end
    , proration_active
    , paid_start
    , paid_end
    , paid_active
    , case

      -- case #2 is just case #5 with a leading, contiguous trial
      -- trial completed and contiguous proration range; may be active or completed
      when
        content_ranges.billing_case = 5 and rails_prod_addon_trial_range_stage.trial_end between content_ranges.proration_start and content_ranges.proration_end
        then 2

      -- case #3 is just case #6 with a leading, contiguous trial
      -- trial comoleted, proration completed, and rolled into billed, may be active or completed
      when
        content_ranges.billing_case = 6 and rails_prod_addon_trial_range_stage.trial_end between content_ranges.proration_start and content_ranges.proration_end
        then 3

      -- case #4 is just case #7 with a leading, contiguous trial
      -- trial completed, and roled into billed range; may be active or completed
      when content_ranges.billing_case = 7 and rails_prod_addon_trial_range_stage.trial_end between content_ranges.paid_start and content_ranges.paid_end
        then 4

      -- pas through remaining cases as is, even if there is a non-contiguous trial joined
      when content_ranges.billing_case in (5, 6, 7)
        then content_ranges.billing_case
    end                                                                                           as billing_case
    , case when billing_case in (2, 3, 4) then rails_prod_addon_trial_range_stage.trial_start end as trial_start
    , case when billing_case in (2, 3, 4) then rails_prod_addon_trial_range_stage.trial_end end   as trial_end
    , case when billing_case in (2, 3, 4) then rails_prod_addon_trial_range_stage.is_active end   as trial_active
  from content_ranges
  left join {{ ref('rails_prod_addon_trial_range_stage') }}
    on content_ranges.account_id = rails_prod_addon_trial_range_stage.account_id
      and content_ranges.package = rails_prod_addon_trial_range_stage.package
  where content_ranges.billing_case is not null

)

, trial_only_ranges as (

  select
    rails_prod_addon_trial_range_stage.account_id
    , rails_prod_addon_trial_range_stage.package
    -- case #1 is just a trial, not contiguous to another range; may be active or completed
    , 1                                            as billing_case
    , rails_prod_addon_trial_range_stage.trial_start
    , rails_prod_addon_trial_range_stage.trial_end
    , rails_prod_addon_trial_range_stage.is_active as trial_active
    , null::timestamp                              as proration_start
    , null::timestamp                              as proration_end
    , false                                        as proration_active
    , null::timestamp                              as paid_start
    , null::timestamp                              as paid_end
    , false                                        as paid_active
  from {{ ref('rails_prod_addon_trial_range_stage') }}
  left join content_ranges
    on rails_prod_addon_trial_range_stage.account_id = content_ranges.account_id
      and rails_prod_addon_trial_range_stage.package = content_ranges.package
      and content_ranges.billing_case is not null
  where
    -- there is no joined content range
    content_ranges.account_id is null

    -- any joined content range is not continguous
    or (content_ranges.proration_start is not null and trial_end < content_ranges.proration_start)
    or (
      content_ranges.proration_start is null and content_ranges.paid_start is not null and trial_end < content_ranges.paid_start
    )

)

, all_ranges as (

  select
    account_id
    , package
    , billing_case
    , trial_start::timestamp
    , trial_end::timestamp
    , coalesce(trial_active, false)     as trial_active
    , proration_start::timestamp
    , proration_end::timestamp
    , coalesce(proration_active, false) as proration_active
    , paid_start::timestamp
    , paid_end::timestamp
    , coalesce(paid_active, false)      as paid_active
  from trial_only_ranges

  union distinct

  select
    account_id
    , package
    , billing_case
    , trial_start::timestamp
    , trial_end::timestamp
    , coalesce(trial_active, false)     as trial_active
    , proration_start::timestamp
    , proration_end::timestamp
    , coalesce(proration_active, false) as proration_active
    , paid_start::timestamp
    , paid_end::timestamp
    , coalesce(paid_active, false)      as paid_active
  from contiguous_ranges

)

select
  *
  , case
    when paid_end is not null and paid_active is true
      then
        'billed'
    when proration_end is not null and proration_active is true
      then
        'proration'
    when trial_end is not null and trial_active is true
      then
        'trial'
    else
      'churned'
  end as current_phase
  , case
    when billing_case = 1
      then
        'trial'
    when billing_case = 2
      then
        'trial->proration'
    when billing_case = 3
      then
        'trial->proration->paid'
    when billing_case = 4
      then
        'trial->paid'
    when billing_case = 5
      then
        'proration'
    when billing_case = 6
      then
        'proration->paid'
    when billing_case = 7
      then
        'paid'
    else
      'unknown'
  end as case_name
from all_ranges

