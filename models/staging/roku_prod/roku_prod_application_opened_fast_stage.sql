with

application_opened as (

  select * from {{ ref('roku_prod_application_opened_stage') }}

)

select
  event_id
  , anonymous_id
  , user_id
  , source
  , url
  , visited_at
  , coupon_code
  , reference
  , context_campaign_content_id
  , context_page_referrer
  , context_user_agent
  , context_page_path
  , context_campaign_source
  , context_campaign_name
  , context_campaign_term
  , context_campaign_medium
  , context_campaign_content
  , has_fast_account_request
  , context_ip
  , visit_type
  , priority
from application_opened
where has_fast_account_request is true