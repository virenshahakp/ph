{{ config(
    materialized='incremental'
    , unique_key='partition_date'
    , sort=['partition_date', 'ended_at', 'platform', 'app_version', 'as_number', 'geohash4']
    , dist='partition_date'
    , full_refresh = false
    , tags=["exclude_daily", "exclude_hourly"]
    , enabled=false
) }}
-- was "daily" tagged, excluding while working through debugging

-- Pull in default vars FROM dbt_project.yml
{% set partition_size = 'day' %}
{% set start_date %}'{{ var("start_date") }}'{% endset %}
{% set end_date %}'{{ var("end_date") }}'{% endset %}

{% if 
    is_incremental()
    and start_date != "''"
    and end_date != "''"
%}
    --ignore min and max CTEs WHEN running backfill

{% elif 
    is_incremental()
    and start_date == "''"
    and end_date == "''"
%}
    --set max partition_date WHEN needed for normal incremental run
    {% set max_partition_date%}
        {{ max_time(field_name = "partition_date", datepart = partition_size, tbl = "") }}
    {% endset %}

{% else %}
    --set min partition_date WHEN needed for --full refresh run
    {% set min_partition_date%}
        {{ min_time(field_name = "DATE_TRUNC('day', ended_at)", datepart = "day", tbl = ref('fct_playback_sessions')) }}
    {% endset %}

{%- endif -%}

SELECT
    DATE_TRUNC('day', ended_at) AS partition_date
    , DATE_TRUNC('day', ended_at) AS ended_at
    , user_id
    , platform
    , app_version
    , as_number
    , as_name
    , SUBSTRING(geohash, 1, 4) AS geohash4
    , manifest_environment
    , COUNT(CASE WHEN stream_error_COUNT > 0 THEN 1 END) AS sessions_with_error
    , COUNT(CASE WHEN rebuffering_start_COUNT > 0 THEN 1 END) AS sessions_with_rebuffering
    , COUNT(CASE WHEN rebuffering_start_COUNT > 1 THEN 1 END) AS sessions_with_rebuffering_1
    , COUNT(CASE WHEN rebuffering_duration_total > 5 THEN 1 END) AS sessions_with_rebuffering_by_duration_5
    , COUNT(CASE WHEN startup_duration_max > 5 THEN 1 END) AS sessions_with_high_startup_5
    , COUNT(CASE WHEN startup_duration_max > 10 THEN 1 END) AS sessions_with_high_startup_10
    -- we see negative rebuffereing durations, treat them AS problems
    , COUNT(CASE 
        WHEN 
            stream_error_COUNT = 0 
            AND rebuffering_duration_total >= 0 
            AND rebuffering_duration_total <= 5 
            AND startup_duration_max <= 10 
        THEN 1 
        END) AS sessions_perfect
    , COUNT(1) AS sessions_total
FROM {{ ref('fct_playback_sessions') }} AS fct_playback_sessions
WHERE 1=1

    /*
    ensures that processing of a day will only occur after that day
    is complete
    */

    AND partition_date::DATE < CURRENT_DATE
    AND {{ dbt.datediff("partition_date::TIMESTAMP", "SYSDATE", 'hour') }} >= 26

    {% if 
        is_incremental()
        and start_date != "''"
        and end_date != "''"
    %}
        /*
        tbl refresh for specific dates. akin to partition overwrites. 
        requires use of command line vars with a normal --run command
        */

        AND partition_date::DATE >= {{ start_date }}
        AND partition_date::DATE < {{dbt.dateadd(
            datepart = partition_size
            , interval = 1
            , from_date_or_timestamp = start_date
        )}}

    {% elif 
        is_incremental()
        and start_date == "''"
        and end_date == "''"
    %}

        /*
        incremental fill logic.
        */

        AND partition_date::DATE >= {{dbt.dateadd(
            datepart = partition_size
            , interval = 1
            , from_date_or_timestamp = max_partition_date
        )}}


    {% else %}

        /*
        starting seed fill logic. ensures that only 1 partition size of data 
        is filled with each run starting with the first partition in source.
        */

        AND partition_date::DATE >= {{ min_partition_date }}
        AND partition_date::DATE < {{dbt.dateadd(
            datepart = partition_size
            , interval = 1
            , from_date_or_timestamp = min_partition_date
        )}}

    {% endif %}

{{ dbt_utils.group_by(n=9) }}
ORDER BY ended_at DESC
