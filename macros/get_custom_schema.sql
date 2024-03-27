/* 
    Override the target schema based upon the environment
    We use this to generate the appropriate schemas in our
    production environment. Details about this mechanism are:
    https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas/

    Also borrowing from the setup that the gitlab team uses:
    https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/utils/generate_schema_name.sql
*/

{% macro generate_schema_name(custom_schema_name, node) -%}
    
  {%- set production_targets = ('prod','docs') -%}

  {#
        Definitions:
            - custom_schema_name: schema provided via dbt_project.yml or model config
            - target.schema: schema provided by the target defined in profiles.yml
            - target.name: name of the target (dev for local development, prod for production, etc.)
        
        This macro is hard to test, but here are some test cases and expected output.
        (custom_schema_name, target.name, target.schema) = <output>

        (analytics, prod, analytics) = analytics
        (analytics, dev, dbt_sziolko) = dbt_sziolko_analytics
        
        (dbt_staging, prod, dbt_staging) = dbt_staging        
        (dbt_staging, dev, dbt_sziolko) = dbt_sziolko_staging

    #}
    {%- if target.name in production_targets -%}
        
        {%- if custom_schema_name is none -%}

            {{ target.schema.lower() | trim }}

        {%- else -%}
    
            {{ custom_schema_name.lower() | trim }}
    
        {%- endif -%}

    {%- else -%}
        
        {%- set default_schema = target.schema -%}
        {%- if custom_schema_name is none -%}

            {{ default_schema }}

        {%- else -%}

            {{ default_schema }}_{{ custom_schema_name | trim }}

        {%- endif -%}

    {%- endif -%}
    
{%- endmacro %}