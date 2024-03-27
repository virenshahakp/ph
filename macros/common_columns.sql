{% macro android_common_columns() %}
  id
  , user_id
  , hashed_session_id
  , CASE
      WHEN "timestamp" BETWEEN {{ var('philo_start_date') }} AND GETDATE()
      THEN "timestamp"
      ELSE received_at
    END                              AS event_timestamp
  , received_at                      AS received_at

  , coalesce(context_app_version, environment_app_version)          AS app_version
  , coalesce(environment_analytics_version, context_environment_analytics_version)    AS analytics_version
  -- Use environment for the os version because on Android we provide a
  -- more details os version string than the segment library
  , coalesce(environment_os_version, context_environment_os_version)           AS os_version
  , context_ip                       AS client_ip
  , context_device_name              AS device_name
  , context_device_manufacturer      AS device_manufacturer
  , context_device_model             AS device_model
{% endmacro %}

{% macro samsung_common_columns() %}
  id
  , anonymous_id
  , user_id
  , hashed_session_id

  , case
    when "timestamp" between {{ var('philo_start_date') }} and getdate()
      then "timestamp"
    else received_at
  end                                as event_timestamp
  , received_at                      as received_at

  , environment_version              as app_version
  , environment_analytics_version    as analytics_version

  , context_ip                       as client_ip
{% endmacro %}

{% macro viziotv_common_columns() %}
  id
  , anonymous_id
  , user_id
  , hashed_session_id

  , case
    when "timestamp" between {{ var('philo_start_date') }} and getdate()
      then "timestamp"
    else received_at
  end                                as event_timestamp
  , received_at                      as received_at

  , environment_version              as app_version
  , environment_analytics_version    as analytics_version

  , context_ip                       as client_ip
{% endmacro %}


{% macro web_common_columns() %}
  id
  , user_id
  , hashed_session_id
  , CASE
      WHEN "timestamp" BETWEEN {{ var('philo_start_date') }} AND GETDATE()
      THEN "timestamp"
      ELSE received_at
    END                                          AS event_timestamp
  , received_at                                  AS received_at

  , environment_version                          AS app_version
  , environment_analytics_version                AS analytics_version

  , {{ parse_user_agent(
      platform_field="'web'",
      user_agent_field="context_user_agent",
      platform_type_field="device_model",
      platform_os_field="os_version",
      browser_field="device_name") }}
  , context_ip                                   AS client_ip
  , NULL                                         AS device_manufacturer
{% endmacro %}

{% macro roku_common_columns() %}
  id
  , user_id
  , hashed_session_id
  , CASE
      WHEN "timestamp" BETWEEN {{ var('philo_start_date') }} AND GETDATE()
      THEN "timestamp"
      ELSE received_at
    END                                          AS event_timestamp
  , received_at                                  AS received_at

  , environment_version              AS app_version
  , environment_analyticsversion     AS analytics_version

  , context_os_version               AS os_version
  , context_ip                       AS client_ip
  , context_device_type              AS device_name
  , NULL                             AS device_manufacturer
  , context_device_model             AS device_model
  -- TODO: Do we care about context_device_model_group
{% endmacro %}

{% macro apple_common_columns() %}
  id
  , LOWER(user_id)                   AS user_id
  , hashed_session_id
  , CASE
      WHEN "timestamp" BETWEEN {{ var('philo_start_date') }} AND GETDATE()
      THEN "timestamp"
      ELSE received_at
    END                                          AS event_timestamp
  , received_at                                  AS received_at

  , context_app_version
      || '-'
      || context_app_build           AS app_version
  , environment_analytics_version    AS analytics_version
  , context_os_version               AS os_version
  , context_ip                       AS client_ip
  , NULL                             AS device_name
  , context_device_manufacturer      AS device_manufacturer
  , context_device_model             AS device_model
{% endmacro %}

{% macro common_columns() %}
  {{ return([
           "id"
            , "user_id"
            , "hashed_session_id"
            , "event_timestamp"
            , "received_at"

            , "app_version"
            , "analytics_version"
            , "os_version"
            , "client_ip"
            , "device_name"
            , "device_manufacturer"
            , "device_model"
      ])
  }}
{% endmacro %}

{%- macro columns_select(columns, skip_columns=[]) -%}
  {%- for col in columns -%}
    {% if not col in skip_columns %}
    {{ col }} as {{ col }}
    {% if not loop.last %}, {% endif %}
    {% endif %}
  {%- endfor -%}
{%- endmacro -%}
