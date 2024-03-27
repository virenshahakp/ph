{{
  config(
    materialized='incremental'
    , dist='anonymous_id'
    , sort=['anonymous_id', 'visited_at']
  )
}}

{%- set max_loaded_at = incremental_max_value('loaded_at') %}

with

application_opened as (

  select * from {{ ref('roku_prod_application_opened_source') }}

)

select
  event_id
  , anonymous_id
  , user_id
  , loaded_at
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
  , 'roku' as visit_type
  , case
    when context_campaign_source is not null then 1
    else 2
  end      as priority
from application_opened
{%- if is_incremental() %}
  where loaded_at > {{ max_loaded_at }}
{%- endif %}