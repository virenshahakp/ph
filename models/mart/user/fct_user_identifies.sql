{{ config(materialized='incremental', sort='timestamp', dist='user_id' ) }}

{%- set platform_dbt_processed_at =
  incremental_max_event_type_value(field_name="loaded_at", event_type="platform")
%}


with

all_identifies as (

  select * from {{ ref('all_platforms_identifies') }}

)

-- noqa: disable=all
-- issue with jinja formatting, passing in dev but not in gitlab
select *
from all_identifies
{%- if is_incremental() %}
  {%- if platform_dbt_processed_at %}
    where
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
  {%- endif %}
{%- endif %}
