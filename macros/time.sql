/*
Preload min time into a dictionary. Useful for models requiring 
backfill functionality and an incrementally built structure
that have not been created yet. Identifies the minimum date_partition 
of source tbl so that the first partition can be queued and processed. \
Works with dbt_backfill shell helper.
*/

{%- macro min_time(field_name = "partition_date", datepart = "day", tbl = "") -%}
    {%- if execute %}
        {%- set min_time_query %}
            WITH min_cte AS (
                SELECT MIN({{ field_name }})::DATETIME AS mintime 
                FROM {%- if tbl == "" %}{{ this }}{%- else %}{{ tbl }}{%- endif %}
            )
            SELECT {{ dbt.date_trunc(datepart = datepart, date = "mintime") }} FROM min_cte
        {%- endset %}
        {%- set results = run_query(min_time_query) %}
        {%- set this_min_time -%}
            '{{ results.columns[0].values()[0] }}'
        {%- endset %}
    {%- endif %}
    {{ return(this_min_time) }}
{%- endmacro -%}

/*
Preload max time into a dictionary. Useful for models requiring 
backfill functionality and an incrementally built structure.
Identifies the maximum date_partition currently loaded so that
the next partition following it can be queued and processed.
Works with dbt_backfill shell helper.
*/

{%- macro max_time(field_name = "partition_date", datepart = "day", tbl = "") -%}
    {%- if execute %}
        {%- set max_time_query %}
            WITH max_cte AS (
                SELECT MAX({{ field_name }})::DATETIME AS maxtime 
                FROM {%- if tbl == "" %}{{ this }}{%- else %}{{ tbl }}{%- endif %}
            )
            SELECT {{ dbt.date_trunc(datepart = datepart, date = "maxtime") }} FROM max_cte
        {%- endset %}
        {%- set results = run_query(max_time_query) %}
        {%- set this_max_time -%}
            '{{ results.columns[0].values()[0] }}'
        {%- endset %}
    {%- endif %}
    {{ return(this_max_time) }}
{%- endmacro -%}



/*
Preloads min partition into the dictionary. Useful for models requiring 
backfill functionality and an incremenatally built structure. 
Identifies the minimum date_partition of an external schema source for
variable references. Works with dbt_backfill function.
*/

{%- macro min_partition(date_index = 0, schema = "", tbl = "") -%}
    {%- if execute %}
        {%- set query %}
        
            SELECT
                json_extract_array_element_text(MIN(values), {{ date_index }})::DATETIME AS focus_partition
            FROM SVV_EXTERNAL_PARTITIONS
            WHERE schemaname = {%- if schema == "" %}'{{ this.schema }}'{%- else %}'{{ schema }}'{%- endif %}
                AND tablename = {%- if tbl == "" %}'{{ this.identifier }}'{%- else %}'{{ tbl }}'{%- endif %}

        {%- endset %}
        {%- set results = run_query(query) %}
        {%- set focus_partition -%}
            '{{ results.columns[0].values()[0] }}'
        {%- endset %}
    {%- endif %}
    {{ return(focus_partition) }}
{%- endmacro -%}


/*
Preloads max partition into the dictionary. Useful for models requiring 
backfill functionality and an incremenatally built structure.
Identifies the maximum date_partition of an external schema source for
variable references. Works with dbt_backfill shell helper.
*/

{%- macro max_partition(date_index = 0, schema = "", tbl = "") -%}
    {%- if execute %}
        {%- set query %}

            SELECT
                json_extract_array_element_text(MAX(values), {{ date_index }})::DATETIME AS focus_partition
            FROM SVV_EXTERNAL_PARTITIONS
            WHERE schemaname = {%- if schema == "" %}'{{ this.schema }}'{%- else %}'{{ schema }}'{%- endif %}
                AND tablename = {%- if tbl == "" %}'{{ this.identifier }}'{%- else %}'{{ tbl }}'{%- endif %}

        {%- endset %}
        {%- set results = run_query(query) %}
        {%- set focus_partition -%}
            '{{ results.columns[0].values()[0] }}'
        {%- endset %}
    {%- endif %}
    {{ return(focus_partition) }}
{%- endmacro -%}
