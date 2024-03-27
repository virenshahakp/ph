{{
  config(
    materialized='incremental'
    , unique_key='version_platform_hashed_id'
    , sort='platform'
    , dist='EVEN'
  )
}}

{%- set platform_first_seen = incremental_max_event_type_value(field_name="first_seen", event_type="platform") %}

with

platform_version_dates as (

  select
    platform
    , app_version
    , md5(nullif(app_version || platform, '')) as version_platform_hashed_id
    , min(loaded_at)                           as first_seen
  from {{ ref('fct_screen_events') }}
  /*
    loaded_at > max_first_seen will repeatedly look at some events on each run until there is a new app version,
    but this incremental/where logic will limit us to looking at only potentially new versions and a subset of screen events
  */
  {%- if is_incremental() %}
    where
      -- only look for new versions
      version_platform_hashed_id not in (select version_platform_hashed_id from {{ this }})

    {%- if platform_dbt_processed_at %}
        and (
            -- ensure that any new platform is included
            platform not in (
              {% for platform in platform_dbt_processed_at['platform'] -%}
                '{{ platform }}'
                {%- if not loop.last %},{%- endif -%}
              {%- endfor %}
            )
          
            {%- for index in range(platform_dbt_processed_at['platform'] | length) -%}
              -- for each existing platform get the incremental timestamp specific to that platform
              or (
                platform = '{{ platform_dbt_processed_at["platform"][index] }}'
                and loaded_at > '{{ platform_dbt_processed_at["max_dbt_processed_at"][index] }}'::timestamp
              )
            {%- endfor -%}
        )
      {%- endif %}
  {%- endif %}
  {{ dbt_utils.group_by(n=3) }}

)

select
  platform
  , app_version
  , version_platform_hashed_id
  , first_seen
  , case
    when platform in ('web', 'chromecast') then split_part(app_version, '-', 1)
    else split_part(app_version, '.', 1)
  end as major_version
  , case
    when platform in ('web', 'chromecast') then split_part(app_version, '-', 2)
    else split_part(app_version, '.', 2)
  end as minor_version
  , case
    when platform in ('web', 'chromecast') then null
    else split_part(split_part(app_version, '.', 3), '-', 1)
  end as patch_version
from platform_version_dates
