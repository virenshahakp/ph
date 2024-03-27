-- how many days of data should be included in dev work
{%- macro incremental_dev_mode_days() -%}
  {{ return(14) }}
{%- endmacro -%}

/*
 * How many days of lookback should be done for overlapping
 * intervals when working incrementally.
 * This is used to accomdate any late arriving or late changing
 * information from a given platform, schema, or user.
 */
{%- macro incremental_recent_days() -%}
  {{ return(10) }}
{%- endmacro -%}

/*
  Preload max value into a dictionary. This is used for received_at
  during incremental update to help query planner reduce search space.
  In wr_incremental query, the planner only used received_at < T1 and left received_at > T2
  for later evaluation; with received_at < T1 only, this requires
  a full scan of the historic table. If T2 is hard coded in the query,
  the planner will take it into account as early as possible.
*/
{%- macro incremental_max_value(field_name="received_at", default_value="'1901-01-01'::timestamp") -%}
  {%- set this_max_value = default_value %}

  {%- if execute and is_incremental() %}
    {%- set max_value_query %}
      select max({{ field_name }}) from {{ this }}
    {%- endset %}
    {%- set results = run_query(max_value_query) %}
    -- an empty table will generate a result but it will not be usable
    {% if results.columns[0].values()[0] is not none %}
    {%- set this_max_value -%}
      '{{ results.columns[0].values()[0].isoformat() }}'::timestamp
    {%- endset %}
    {% endif %}
  {%- endif %}
  {{ return(this_max_value) }}
{%- endmacro -%}


/*
  Preload max value per platform into a dictionary. This is used
  during incremental update to help query planner reduce search space.
*/
{%- macro incremental_max_event_type_value(field_name="dbt_processed_at", event_type="platform") -%}
  {%- set results = {} %}
  {%- if execute and is_incremental() %}
    {%- set max_value_query %}
      select {{ event_type }} as "{{ event_type }}", max({{ field_name }}) as max_dbt_processed_at from {{ this }} group by 1
    {%- endset %}
    {%- set results = dbt_utils.get_query_results_as_dict(max_value_query) %}
  {%- endif %}
  {{ return(results) }}
{%- endmacro -%}

/* 
  To be used with the incremental_max_platform_value() macro to have consistent platform filtering code 
  this requires additional work so that it is executed once the results are populated (at execution not at compile)
  it is included here as a quick template for use with the max_value macro
*/
{%- macro incemental_platform_where_clause(source_table="add_platform", timestamp_field="dbt_processed_at", time_dictionary="platform_dbt_processed_at") -%}
  {% if execute %}
  where 
    -- ensure that a new platform is included
    {{ source_table }}.platform not in (
      {%- for platform in time_dictionary['platform'] -%}
      '{{ platform }}'
      {%- if not loop.last %},{%- endif -%}
      {%- endfor -%}
    )
    -- for each defined platform get the incremental delta
    {% for index in range(time_dictionary['platform'] | length) %}
    or (
      --{{ index }}
      {{ source_table }}.platform = '{{ time_dictionary["platform"][index] }}'
      and {{ source_table }}.{{ timestamp_field }} > '{{ time_dictionary["max_dbt_processed_at"][index] }}'::timestamp
    )
    {% endfor %}
    {% endif %}
  {%- endmacro -%}
