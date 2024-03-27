/*
this is a copy of the is_incremental() macro with an adjustment to work for tuple_incremental
https://github.com/dbt-labs/dbt-adapters/blob/main/dbt/include/global_project/macros/materializations/models/incremental/is_incremental.sql
*/

{% macro is_incremental() %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and (model.config.materialized == 'incremental' or model.config.materialized == 'tuple_incremental')
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}