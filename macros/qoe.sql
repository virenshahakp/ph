{%- macro qoe_android_error_description(description) -%}
  CASE
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException: Unable to connect to https://www.philo.com%' THEN 'connection error to www'
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException: Unable to connect to https://content%' THEN 'connection error to fastly (old)'
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException: Unable to connect to https://prod.cdn-fsty.philo.com%' THEN 'connection error to fastly'
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException: Unable to connect to https://prod.cdn-vdms.philo.com%' THEN 'connection error to vdms'
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException: Unable to connect to https://prod.cdn-cf.philo.com%' THEN 'connection error to cf'
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException: Unable to connect to https://prod-m.cdn-cf.philo.com%' THEN 'connection error to cf manifest'
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException: java.net.ProtocolException: java.net.SocketTimeoutException%' THEN 'connection timeout'
    WHEN {{ description }} LIKE '%Response code: 403%' THEN '403'
    WHEN {{ description }} LIKE '%Response code: 404%' THEN '404'
    WHEN {{ description }} LIKE '%Response code: 412%' THEN '412'
    WHEN {{ description }} LIKE '%Response code: 500%' THEN '500'
    WHEN {{ description }} LIKE '%Response code: 502%' THEN '502'
    WHEN {{ description }} LIKE '%Response code: 504%' THEN '504'
    WHEN {{ description }} LIKE '%HttpDataSource$HttpDataSourceException:%' THEN 'other http error'
    WHEN {{ description }} LIKE '%com.google.android.exoplayer2.source.BehindLiveWindowException%' THEN 'behind live'
    WHEN {{ description }} LIKE '%Failed to instantiate crypto object%' THEN 'failed to intantiate crypto'
    WHEN {{ description }} LIKE '%DecoderInitializationException: Decoder init failed:%' THEN 'decoder init failed'
    WHEN {{ description }} LIKE '%Failed to open session: General DRM error%' THEN 'general DRM error'
    WHEN {{ description }} LIKE '%Cause: java.lang.IllegalStateException%' THEN 'decoder error'
    WHEN {{ description }} LIKE '%android.media.MediaCodec$CryptoException%' THEN   'media codec crypto'
    WHEN {{ description }} LIKE '%java.net.UnknownHostException: Unable to resolve host%' THEN 'DNS error'
    WHEN {{ description }} LIKE '%Cause: java.lang.ArrayIndexOutOfBoundsException%' THEN 'array index out of bounds'
    WHEN {{ description }} LIKE '%java.lang.IndexOutOfBounds%' THEN 'index out of bounds (not array)'
    WHEN {{ description }} LIKE '%DashManifestStaleException%' THEN 'dash manifest stale'
    WHEN {{ description }} LIKE '%com.google.android.exoplayer2.ParserException: No periods found.%' THEN 'no periods found'
    WHEN {{ description }} LIKE '%MediaDrm obj is null%' THEN 'media drm obj is null'
    WHEN {{ description }} LIKE '%okhttp3.internal.http2.StreamResetException%' THEN 'http2 stream reset'
    WHEN {{ description }} LIKE '% DRM vendor-defined error%' THEN 'drm vendor-defined error'
    WHEN {{ description }} LIKE '%NotProvisionedException%' THEN 'not provisioned exception'
    WHEN {{ description }} LIKE '%IllegalArgumentException%' THEN 'illegal argument exception'
    WHEN {{ description }} LIKE '%media server died%' THEN 'media server died'
    WHEN {{ description }} LIKE '%android.media.MediaCodec$CodecException%' THEN 'codec exception'
    WHEN {{ description }} LIKE '%ParserException: org.xmlpull.v1.XmlPullParserException' THEN 'xml parse failed'
    WHEN {{ description }} LIKE '%Unable to connect to https://lic.drmtoday.com/license-proxy-widevine/cenc/%' THEN 'connection error to drmtoday'
    WHEN {{ description }} LIKE '%mediaserver died%' THEN 'mediaserver died (drm)'
    WHEN {{ description }} LIKE '%Failed to get provision request%' THEN 'failed to get drm provision request'
    ELSE 'other'
  END
{%- endmacro -%}

{%- macro qoe_roku_error_description(description) -%}
  CASE
    WHEN {{ description }} ILIKE '%An unexpected problem (but not server timeout or HTTP error) has been detected.%'
        THEN 'unexpected problem (HTTP Status code)'
    WHEN {{ description }} ILIKE '%Underrun when network is down.'
        THEN 'underrun when network is down'
    WHEN {{ description }} ILIKE '%Network error.%'
        THEN 'Network error'
    WHEN {{ description }} ILIKE '%There was an error in the HTTP response.%'
        THEN 'there was an error in the HTTP response'
    WHEN {{ description }} ILIKE '%No streams were provided for playback%'
        THEN 'no streams were provided for playback'
    WHEN {{ description }} ILIKE '%Protected content license error.%'
        THEN 'protected content license error'
    WHEN {{ description }} ILIKE '%The connection timed out.%'
        THEN 'connection timeout'
    WHEN {{ description }} ILIKE '%Excessive av skew%'
        THEN 'excessive av skew'
    WHEN {{ description }} ILIKE '%Found empty SegmentTimeline%'
        THEN 'found empty SegmentTimeline'
    WHEN {{ description }} ILIKE '%no valid bitrates%'
        THEN 'no valid bitrates'
    WHEN {{ description }} ILIKE '%malformed data%error:uninitialized%'
        THEN 'write error: uninitialized'
    WHEN {{ description }} ILIKE '%malformed data&errorStr:buffer:loop:demux.dash:Manifest update failed%'
        THEN 'Manifest update failed'
    WHEN {{ description }} ILIKE '%malformed data&errorStr:buffer:loop:dash.period:% select chunk on any bitrate%'
        THEN 'period cannot select chunk on any bitrate'
    WHEN {{ description }} ILIKE '%Missing or invalid Movie Fragment Box%'
        THEN 'missing or invalid movie fragment box'
    WHEN {{ description }} ILIKE '%New periods are empty%'
        THEN 'new periods are empty'
    WHEN {{ description }} ILIKE '%malformed data%is an invalid mpd%'
        THEN 'invalid mpd'
    WHEN {{ description }} ILIKE '%failed to create media player%'
        THEN 'failed to create media player'
    WHEN {{ description }} ILIKE '%A device error occurred%'
        THEN 'a device error occurred'
    WHEN {{ description }} ILIKE '%Expecting %MPD\%, found %html%'
        THEN 'expecting MPD, found HTML'
    WHEN {{ description }} ILIKE '{{ '{%' }}'
        THEN 'other: serialized node'
    WHEN {{ description }} = 'ignored'
        THEN 'ignored'
    WHEN {{ description }} ILIKE '%malformed data%'
        THEN 'other: malformed data'
    ELSE 'other'
  END
{%- endmacro -%}

{%- macro qoe_apple_error_description(description) -%}
  CASE
    WHEN {{ description }} ILIKE '%Media Entry % not match previous playlist%'
        THEN 'media entry does not match previous'
    WHEN {{ description }} ILIKE 'Internal error: restarting too far ahead%'
        THEN 'restarting too far ahead'
    WHEN {{ description }} ILIKE '%Moya.MoyaError%'
            OR {{ description }} = 'Status code didn%t fall within the given range.'
            OR {{ description }} = 'The request timed out.'
        THEN 'moya error'
    WHEN {{ description }} = 'Segment exceeds specified bandwidth for variant'
        THEN 'segment exceeds bandwidth'
    WHEN {{ description }} = 'Can not proceed after removing variant'
        THEN 'can not proceed after removing variant'
    WHEN {{ description }} ILIKE 'No response for media file in%'
        THEN 'no response for media file'
    WHEN {{ description }} ILIKE '%playlist parse error%'
        THEN 'playlist parse error'
    WHEN {{ description }} ILIKE '%Media segment doesn%t match%'
        THEN 'media segment does not match'
    WHEN {{ description }} ILIKE 'Media file not received in%'
        THEN 'media file not received'
    WHEN {{ description }} ILIKE '%Playlist File unchanged%'
        THEN 'playslist file unchanged'
    When {{ description }} ILIKE 'Playlist File not received%'
        THEN 'playlist file not received'
    WHEN {{ description }} ILIKE 'The internet connection appears to be offline.'
            OR {{ description }} ILIKE 'A server with the specified hostname%'
            OR {{ description }} = 'The network connection was lost.'
            OR {{ description }} = 'Could not connect to the server.'
            OR {{ description }} = 'International roaming is currently off.'
            OR {{ description }} = 'A data connection cannot be established since a call is currently active.'
        THEN 'internet connection or dns error'
    WHEN {{ description }} ILIKE '%SSL error%' OR {{ description }} ILIKE 'The certificate for this server is invalid%'
        THEN 'ssl error'
    WHEN {{ description }} ILIKE '{{ '-%' }}'
        THEN {{ description }}
    WHEN {{ description }} ILIKE '%HTTP 400%'
        THEN '400'
    WHEN {{ description }} ILIKE '%HTTP 403%'
        THEN '403'
    WHEN {{ description }} ILIKE '%HTTP 404%'
        THEN '404'
    WHEN {{ description }} ILIKE '%HTTP 416%'
        THEN '416'
    WHEN {{ description }} ILIKE '%HTTP 502%'
        THEN '502'
    WHEN {{ description }} ILIKE '%HTTP 504%'
        THEN '504'
    WHEN {{ description }} = 'crypt key received slowly'
        THEN {{ description }}
    ELSE 'other'
  END
{%- endmacro -%}

{%- macro android_qoe_source_columns() -%}
  {{ android_common_columns() }}
  , event
  , sdpid                            as playback_session_id
  , {{ normalize_id() }}             as asset_id
  , coalesce(position_ms / 1000.0, position) as position
  , bitrate                          as adapted_bitrate
  , null                             as user_selected_bitrate
  , null                             as estimated_bandwidth

  , context_network_wifi             as is_wifi
  , context_network_cellular         as is_cellular

  , context_screen_height            as screen_height
  , context_screen_width             as screen_width

  , uuid_ts                          as loaded_at
{%- endmacro -%}

{%- macro web_qoe_source_columns(chromecast=False) -%}
  {{ web_common_columns() }}
  , event
  , sdpid                            as playback_session_id
  , {{ normalize_id() }}             as asset_id
  , position
  , bitrate                          as adapted_bitrate
  {% if chromecast %}
  , null                             as user_selected_bitrate
  {% else %}
  , user_selected_bitrate
  {% endif %}
  , estimated_bandwidth
  , null::boolean                    as is_wifi
  , null::boolean                    as is_cellular

  , null                             as screen_height
  , null                             as screen_width
  , uuid_ts                          as loaded_at
{%- endmacro -%}

{%- macro samsung_qoe_source_columns() -%}
  {{ samsung_common_columns() }}
  , event
  , sdpid                           as playback_session_id
  , {{ normalize_id() }}            as asset_id
  -- not all events use the same name for position, so we included at source, not macro 
  , null                            as adapted_bitrate
  , null                            as user_selected_bitrate
  , null                            as estimated_bandwidth

  , null::boolean                   as is_wifi
  , null::boolean                   as is_cellular

  , null                            as screen_height
  , null                            as screen_width
  , uuid_ts                         as loaded_at
  , md5(nullif(trim(context_user_agent), ''))     as context_user_agent_id
{%- endmacro -%}

{%- macro viziotv_qoe_source_columns() -%}
  {{ viziotv_common_columns() }}
  , event
  , sdpid                           as playback_session_id
  , {{ normalize_id() }}            as asset_id
  -- not all events use the same name for position, so we included at source, not macro 
  , null                            as adapted_bitrate
  , null                            as user_selected_bitrate
  , null                            as estimated_bandwidth

  , null::boolean                   as is_wifi
  , null::boolean                   as is_cellular

  , null                            as screen_height
  , null                            as screen_width
  , uuid_ts                         as loaded_at
  , md5(nullif(trim(context_user_agent), ''))     as context_user_agent_id
{%- endmacro -%}


{%- macro roku_qoe_source_columns() -%}
  {{ roku_common_columns() }}
  , event
  , sdpid                            as playback_session_id
  , {{ normalize_id() }}             as asset_id
  , coalesce(position_ms / 1000.0, position) as position
  , null                             as adapted_bitrate
  , null                             as user_selected_bitrate
  , null                             as estimated_bandwidth
  , CAST(null as BOOLEAN)            as is_wifi
  , CAST(null as BOOLEAN)            as is_cellular

  , context_screen_height            as screen_height
  , context_screen_width             as screen_width
  , uuid_ts                          as loaded_at
{%- endmacro -%}

{%- macro apple_qoe_source_columns() -%}
  {{ apple_common_columns() }}
  , event
  , sdpid                            as playback_session_id
  , {{ normalize_id() }}             as asset_id
  , coalesce(position_ms / 1000.0, position) as position
  , bitrate                          as adapted_bitrate
  , null                             as user_selected_bitrate
  , null                             as estimated_bandwidth
  , context_network_wifi             as is_wifi
  , context_network_cellular         as is_cellular

  , context_screen_height            as screen_height
  , context_screen_width             as screen_width
  , uuid_ts                          as loaded_at
{%- endmacro -%}


{%- macro qoe_columns(additional_columns=[]) -%}
  {{ return(common_columns() + [
            "event"
            , "playback_session_id"
            , "asset_id"
            , "adapted_bitrate"
            , "user_selected_bitrate"
            , "estimated_bandwidth"
            , "is_wifi"
            , "is_cellular"

            , "position"
            , "position_ms"

            , "screen_height"
            , "screen_width"
            , "loaded_at"
      ] + additional_columns)
  }}
{%- endmacro -%}

{%- macro qoe_columns_select(skip_columns=[]) -%}
  {{ columns_select(qoe_columns(), skip_columns) }}
{%- endmacro -%}

{%- macro qoe_event_id() -%}
  {{ dbt_utils.generate_surrogate_key(['event', 'id']) }} as event_id
{%- endmacro -%}
