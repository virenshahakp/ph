{%- macro get_platform_from_union_relations(_dbt_source_relation) -%}

/*
  use the union_relations output and our convention to name event
  schemas as "device_prod_..." to extract the platform from the
  event source, removing any "_" prior to "_prod" to be consistent
  with other platform identification.

  The inner "SPLIT_PART" function pulls the table name out as it 
  is the third element of "database"."schema"."tablename" and the second
  SPLIT_PART pulls the device name from the table.
*/

( 
  REPLACE( -- remove underscores from platform name
    SPLIT_PART( -- get text preceeding "_prod" in model name
      SPLIT_PART( -- get table name from relation
        _dbt_source_relation
        , '."'
        , 3
      )
      , '_prod'
      , 1
    )
    , '_'
    , ''
  )
)

{%- endmacro %}

{%- macro get_platform_from_playback_session(playback_session_id) -%}

/*
  use the leading part of the playback_session_id to define the platform

  we use decode to remap to names that are consistent with the output of
  get_platform_from_union_relations() which does not always align
  with the values from dataserver

  platforms reported at the start of playback_session_ids
  android
  androidtv
  chromecast
  fire
  firetv
  ios
  roku
  samsungtv
  tvos
  viziotv
  web

  ids are expected to be of the form:
  androidtv-500776f7-c937-4483-a85a-07cfa401c456
  platformname-uuid

  so we split on the first element delimited by '-'
  make it lowercase to be case-insensitive
  then remap to our desired strings where necessary

  values not in this list will be null
*/

decode(
  lower(split_part( {{ playback_session_id }}, '-', 1)),
  'android', 'android',
  'androidtv', 'androidtv',
  'chromecast', 'chromecast',
  'fire', 'fire',
  'firetv', 'firetv',
  'ios', 'ios',
  'roku', 'roku',
  'samsungtv', 'samsung',
  'tvos', 'tvos',
  'viziotv', 'viziotv',
  'web', 'web'
)

{%- endmacro %}