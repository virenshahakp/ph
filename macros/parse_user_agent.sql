{%- macro parse_user_agent(
  platform_field="platform",
  user_agent_field="context_user_agent",
  platform_type_field="platform_type",
  platform_os_field="platform_os",
  browser_field="browser") -%}

  CASE
    WHEN {{ platform_field }} IN ('androidtv', 'fire', 'firetv', 'roku', 'samsung', 'tvos', 'viziotv')
    THEN 'tv-streaming-device'
    WHEN {{ platform_field }} IN ('android')
    THEN 'android'
    WHEN {{ platform_field }} IN ('ios')
    THEN 'ios'
    WHEN {{ platform_field }} = 'web'
    THEN
      CASE
        WHEN {{ user_agent_field }} ~* ('iphone|ipad')
        THEN 'ios-'
        WHEN {{ user_agent_field }} ilike '%android%'
        THEN 'android-'
        ELSE ''
      END || 'web'
    ELSE {{ platform_field }}
  END AS {{ platform_type_field }}
, CASE
    WHEN {{ platform_field }} IN ('androidtv', 'fire', 'firetv')
    THEN 'androidtv'
    WHEN {{ platform_field }} IN ('android', 'ios', 'roku', 'samsung', 'tvos', 'viziotv')
    THEN {{ platform_field }}
    WHEN {{ platform_field }} = 'web'
    THEN
      CASE
        WHEN {{ user_agent_field }} ILIKE '%iPad%'
        THEN 'iPad'
        WHEN {{ user_agent_field }} ILIKE '%iPhone%'
        THEN 'iPhone'
        WHEN {{ user_agent_field }} ILIKE '%Mac%OS%'
        THEN 'Mac OS X'
        WHEN {{ user_agent_field }} ILIKE '%android%'
        THEN 'android'
        WHEN {{ user_agent_field }} ILIKE '%linux%'
        THEN 'linux'
        WHEN {{ user_agent_field }} ILIKE '%win%'
        THEN 'Windows'
        WHEN {{ user_agent_field }} ILIKE '%bot%'
        THEN 'Bot'
        WHEN {{ user_agent_field }} ILIKE '%http://%'
        THEN 'Bot'
        WHEN {{ user_agent_field }} ILIKE '%Wget%'
        THEN 'Bot'
        WHEN {{ user_agent_field }} ILIKE '%curl%'
        THEN 'Bot'
        WHEN {{ user_agent_field }} ILIKE '%urllib%'
        THEN 'Bot'
        ELSE 'Other'
      END
    ELSE 'Unknown'
  END AS {{ platform_os_field }}
, CASE
    WHEN {{ user_agent_field }} ILIKE '%edge%'
    THEN 'Edge'
    WHEN {{ user_agent_field }} ILIKE '%MSIE%'
    THEN 'Internet Explorer'
    WHEN {{ user_agent_field }} ILIKE '%Firefox%'
    THEN 'Firefox'
    WHEN {{ user_agent_field }} ILIKE '%Chrome%' OR {{ user_agent_field }} ILIKE '%CriOS%'
    THEN 'Chrome'
    WHEN {{ user_agent_field }} ILIKE '%Safari%'
    THEN 'Safari'
    WHEN {{ user_agent_field }} ILIKE '%Opera%'
    THEN 'Opera'
    WHEN {{ user_agent_field }} ILIKE '%Outlook%'
    THEN 'Outlook'
    WHEN {{ user_agent_field }} ILIKE '%Facebook%'
    THEN 'Facebook'
    ELSE {{ platform_field }}
  END AS {{ browser_field }}

{%- endmacro %}
