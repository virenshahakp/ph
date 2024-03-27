with

all_platforms as (

  select distinct *
  from
    {{ dbt_utils.union_relations(
        relations=[
          ref('android_prod_attributed_installs')
          , ref('android_prod_application_opened_stage')
          , ref('androidtv_prod_application_opened_stage')
          , ref('fire_prod_application_opened_stage')
          , ref('fire_tv_prod_attributed_installs')
          , ref('fire_tv_prod_application_opened_stage')
          , ref('ios_prod_attributed_installs')
          , ref('ios_prod_application_opened_stage')
          , ref('rails_prod_attributed_visits')
          , ref('roku_prod_attributed_installs')
          , ref('roku_prod_application_opened_paid_stage')
          , ref('samsung_prod_attributed_installs')
          , ref('samsung_prod_application_opened_stage')
          , ref('tvos_prod_application_opened_stage')
          , ref('viziotv_prod_pages_stage')
          , ref('viziotv_prod_attributed_installs')
          , ref('web_prod_pages_stage')
        ],
        include=[
          "anonymous_id"
          , "user_id"
          , "context_ip"
          , "context_campaign_source"
          , "context_campaign_name"
          , "context_campaign_term"
          , "context_campaign_medium"
          , "context_page_referrer"
          , "context_user_agent"
          , "context_page_path"
          , "context_campaign_content"
          , "context_campaign_content_id"
          , "url"
          , "visited_at"
          , "priority"
          , "visit_type"
          , "coupon_code"
          , "reference"
        ]
      ) 
    }}

)

, add_platform as (

  select
    *
    , {{ get_platform_from_union_relations(_dbt_source_relation) }} as platform
  from all_platforms

)

select
  anonymous_id
  , user_id
  , context_ip
  , context_campaign_source
  , context_campaign_name
  , context_campaign_term
  , context_campaign_medium
  , context_page_referrer
  , context_user_agent
  -- generate platform_type, platform_os, and browser fields
  -- the platform_field and user_agent_field are used in parsing
  -- the other fields are the names of the output columns
  , {{ parse_user_agent(
      platform_field="platform",
      user_agent_field="context_user_agent",
      platform_type_field="platform_type",
      platform_os_field="platform_os",
      browser_field="browser") }}
  -- remove any '/' in the path (leading or otherwise) make the text lowercase and then 
  -- put the '/' back in front of the path. All done to make querying/processing simpler
  , '/' + lower(replace(context_page_path, '/', '')) as context_page_path
  , context_campaign_content
  , context_campaign_content_id
  , url
  , visited_at
  , priority
  , visit_type
  , coupon_code
  -- this CASE uses concatenation property that if context_campaign_source is null, 
  -- then the resulting string will always be null
  -- AP: TODO: what do we allow a null context_campaign_source to make traffic_type null? 
  -- Do we filter out this traffic in other graphs?
  , case
    when
      context_campaign_source is null
      and context_campaign_name != 'share'
      and context_campaign_term is not null
      and context_user_agent != 'roku'
      -- AP: ??? what was this trying to do? I added the name != 'share' to make an exception for share visits
      then 'unknown source'
    when
      context_campaign_source is null
      and context_page_referrer ilike '%facebook.%'
      then 'organic facebook'
    when
      context_campaign_source is null
      and context_page_referrer ilike '%google.%'
      then 'organic google'
    when
      context_campaign_source is null
      and context_page_referrer ilike '%bing.%'
      then 'organic bing'
    when
      context_campaign_source is null
      and context_page_referrer ilike '%yahoo.%'
      then 'organic yahoo'
    when
      context_campaign_source is null
      and context_page_referrer ilike '%youtube.%'
      then 'organic youtube'
    when
      context_campaign_source is null
      and context_campaign_term = 'philo'
      then 'organic roku' -- AP: ???
    when
      coalesce(trim(context_campaign_source), '') = ''
      and coalesce(context_page_referrer, '') = ''
      then 'direct'
    when context_campaign_source ilike 'cj%'
      then 'cj'
    when coupon_code is not null
      then 'coupon'
    when
      coalesce(trim(context_campaign_source), '') = ''
      and context_page_referrer is not null
      then 'organic other'
    else lower(context_campaign_source)
  end
  || ' '
  || case
    when context_campaign_term is null then ''
    when context_campaign_term ilike '%philo%' then 'philo'
    -- this is our ad agency Decoded running on our behalf
    when context_campaign_term ilike 'dc-%' then 'philo'
    else 'partner'
  end
  || ' '
  || case
    when context_campaign_name is null then ''
    when context_campaign_name = 'philo' then 'brand'
    -- when context_campaign_name = 'share' then 'content' -- AP: is this the right way to name share links?
    else 'content'
  end                                                as traffic_type
  , reference
  , platform
from add_platform
