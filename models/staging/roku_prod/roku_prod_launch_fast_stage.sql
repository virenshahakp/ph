with

launch as (

  select * from {{ ref('roku_prod_launch_stage') }}

)

select
  event_id
  , anonymous_id
  , visited_at
  , visit_type
  , has_fast_account_request
  , url
  , coupon_code
  , reference
  , priority
  , context_ip
  , context_campaign_source
  , context_campaign_name
  , context_campaign_term
  , context_campaign_medium
  , context_page_referrer
  , context_user_agent
  , context_page_path
  , context_campaign_content
  , context_campaign_content_id
from launch
where has_fast_account_request is true