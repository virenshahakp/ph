
/*
    This file contains the nessesary macros to run the tuple_intremental materialization.  Most of
    the contained macros are aliased copies of native dbt macros.  It is nessesary to create 
    copies to resolve the issue of custom materializations being unableable to reference these native 
    macros. The copies are then aliased to prevent conflficts with the native dbt macros in case 
    they are updated in future versions of dbt.

    None of the macros contained in this file overwrite core dbt macros. 

*/ 


--this is a copy of adapters.sql make_backup_relation
{% macro make_backup_relation_tuple(base_relation, backup_relation_type, suffix='__dbt_backup') %}
    {{ return(adapter.dispatch('make_backup_relation_tuple', 'dbt')(base_relation, backup_relation_type, suffix)) }}
{% endmacro %}


--this is a copy of adapters.sql post_gres__make_backup_relation
{% macro postgres__make_backup_relation_tuple(base_relation, backup_relation_type, suffix) %}
    {% set backup_relation = postgres__make_relation_with_suffix_tuple(base_relation, suffix, dstring=False) %}
    {{ return(backup_relation.incorporate(type=backup_relation_type)) }}
{% endmacro %}


--this is a copy of adapters.sql postgres__make_relation_with_suffix
{% macro postgres__make_relation_with_suffix_tuple(base_relation, suffix, dstring) %}
    {% if dstring %}
      {% set dt = modules.datetime.datetime.now() %}
      {% set dtstring = dt.strftime("%H%M%S%f") %}
      {% set suffix = suffix ~ dtstring %}
    {% endif %}
    {% set suffix_length = suffix|length %}
    {% set relation_max_name_length = 63 %}
    {% if suffix_length > relation_max_name_length %}
        {% do exceptions.raise_compiler_error('Relation suffix is too long (' ~ suffix_length ~ ' characters). Maximum length is ' ~ relation_max_name_length ~ ' characters.') %}
    {% endif %}
    {% set identifier = base_relation.identifier[:relation_max_name_length - suffix_length] ~ suffix %}

    {{ return(base_relation.incorporate(path={"identifier": identifier })) }}

  {% endmacro %}

--this generates the sql to build a table which contain only the unique combinations to delete. It is custom
{% macro get_delete_sql(unique_key, temp_relation) -%}
    select distinct {% for key in unique_key %} 
        {{ key }} 
        {{ ", " if not loop.last }} 
        {% endfor %} 
        from {{ temp_relation }}
{%- endmacro %}

--This calls the adapter specific version of delete_create_table_as. It is custom
{% macro delete_get_create_table_as_sql(temporary, relation, sql) -%}
  {{ adapter.dispatch('delete_create_table_as', 'dbt')(temporary, relation, sql) }}
{%- endmacro %}

--this is a copy of the redshift__create_table_as function.  We need to create a modified version that removes dist and sort
{% macro redshift__delete_create_table_as(temporary, relation, sql) -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set backup = config.get('backup') -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary), schema=(not temporary)) }}
    {% if backup == false -%}backup no{%- endif %}
  as (
    {{ sql }}
  );
{%- endmacro %}


--this is mostly a copy of adapters.sql get_delete_insert_merge_sql. An additional argument, delete_touple, which is the delete table, is provided
{% macro get_delete_insert_merge_sql_touple(target, source, unique_key, dest_columns, delete_touple) -%}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete from {{target }}
            using {{ delete_touple }}
            where (
                {% for key in unique_key %}
                    {{ delete_touple }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last }}
                {% endfor %}
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select distinct ({{ unique_key }})
                from {{ source }}
            );

        {% endif %}
        {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{%- endmacro %}