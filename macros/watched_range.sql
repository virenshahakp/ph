{%- macro watched_range_columns() -%}
  {{ return([
              "user_id"
            , "playback_session_id"
            , "requested_asset_id"
            , "played_asset_id"
            , "received_at"
            , "timestamp_start"
            , "timestamp"
            , "delay"
            , "position_start"
            , "position_stop"
            , "hashed_session_id"
            , "context_ip"
            , "platform"
            , "channel_id"
            , "show_id"
            , "episode_id"
            , "run_time"
            , "tms_series_id"
            , "philo_series_id"
            , "bitrate"
  ]) }}
{%- endmacro -%}

{%- macro watched_range_select() -%}
  {%- for col in watched_range_columns() -%}
    "{{ col }}" as "{{ col }}"
    {% if not loop.last %}, {% endif %}
  {%- endfor -%}
{%- endmacro -%}

