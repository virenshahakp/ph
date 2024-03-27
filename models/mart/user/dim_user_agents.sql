{{ 
  config(
    materialized='incremental'
    , unique_key='context_user_agent_id'
    , dist='context_user_agent_id'
    , enabled=true
  )
}}

with

web_user_agents as (

  select
    context_user_agent_id
    -- remove non-ascii characters from parsing
    , left(regexp_replace(context_user_agent, '[^[:alnum:][:blank:][:punct:]]', ''), 512) as context_user_agent
  from {{ ref('web_prod_pages_stage') }}

)

, samsung_user_agents as (

  select
    context_user_agent_id
    -- remove non-ascii characters from parsing
    , left(regexp_replace(context_user_agent, '[^[:alnum:][:blank:][:punct:]]', ''), 512) as context_user_agent
  from {{ ref('samsung_prod_pages_stage') }}

)

, vizio_user_agents as (

  select
    context_user_agent_id
    -- remove non-ascii characters from parsing
    , left(regexp_replace(context_user_agent, '[^[:alnum:][:blank:][:punct:]]', ''), 512) as context_user_agent
  from {{ ref('viziotv_prod_pages_stage') }}

)

, user_agents as (

  select * from web_user_agents
  union distinct
  select * from samsung_user_agents
  union distinct
  select * from vizio_user_agents

)

{% if is_incremental() %}
  , existing_lookups as (

    select context_user_agent_id from {{ this }}

  )
{% endif %}

, to_parse as (

  select
    context_user_agent_id
    , context_user_agent
  from user_agents
  {%- if is_incremental() %}
    where user_agents.context_user_agent_id not in (select context_user_agent_id from existing_lookups)
  {%- endif %}
  -- limit is being applied as python UDFs are resource intensive, if volume changes or useragents become a lot more
  -- unique then this limit will need to be changed.
  limit 100000

)

, parsed as (

  select
    context_user_agent_id
    , context_user_agent
    -- doing the parsing in a CTE after limiting records has a performance benefit 
    -- due to Redshift Python UDF implementation
    , f_parse_ua_as_json(context_user_agent) as ua_json
  from to_parse

)

select distinct
  context_user_agent_id
  , context_user_agent

  , json_extract_path_text(ua_json, 'device', 'family')     as device_family
  , json_extract_path_text(ua_json, 'device', 'brand')      as device_brand
  , json_extract_path_text(ua_json, 'device', 'model')      as device_model

  , json_extract_path_text(ua_json, 'os', 'family')         as os_family
  , json_extract_path_text(ua_json, 'os', 'major')          as os_version_major
  , json_extract_path_text(ua_json, 'os', 'minor')          as os_version_minor
  , json_extract_path_text(ua_json, 'os', 'patch')          as os_version_patch

  , json_extract_path_text(ua_json, 'user_agent', 'family') as browser_family
  , json_extract_path_text(ua_json, 'user_agent', 'major')  as browser_version_major
  , json_extract_path_text(ua_json, 'user_agent', 'minor')  as browser_version_minor
  , json_extract_path_text(ua_json, 'user_agent', 'patch')  as browser_version_patch

from parsed