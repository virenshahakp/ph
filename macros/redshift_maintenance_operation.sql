{% macro vacuumable_tables_sql() %}
{%- set limit=kwargs.get('limit') -%}
{%- set max_size_tb=kwargs.get('max_size_tb') -%}
{%- set unsorted_threshold=kwargs.get('unsorted_threshold') -%}
{%- set stats_threshold=kwargs.get('stats_threshold') -%}

-- usage: dbt run-operation redshift_maintenance --args '{limit: 1, max_size_tb: 1, unsorted_threshold: 10, stats_threshold: 20}'
select
  database  as table_database
  , schema  as table_schema
  , "table" as table_name
from SVV_TABLE_INFO
where 
  -- only maintain high use, production schemas --
  {{ include_redshift_prod_schemas() }}
  -- don't maintain tables that are too large --
  and size < {{ max_size_tb | default(2, true) }}*1024*1024 -- size in MB
  -- only list tables that need maintaining --
  and ( 
    unsorted > {{ unsorted_threshold | default(5, true) }}
    or stats_off > {{ stats_threshold | default(10, true) }}
  )
order by vacuum_sort_benefit desc -- vacuum highest benefit tables first
{% if limit %}
limit ~ {{ limit }}
{% endif %}
{% endmacro %}