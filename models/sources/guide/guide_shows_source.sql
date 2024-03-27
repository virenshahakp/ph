with

shows as (

  select * from {{ source('guide', 'shows') }}

)

, renamed as (

  select
    id                          as show_id
    , {{ normalize_id("_id") }}                             as asset_id
    , lower(title)              as show_title
    , lower(entity_type)        as content_type
    , has_public_view           as has_public_view
    , orig_air_date::date       as original_air_date
    , is_tmsid_valid            as is_tmsid_valid
    , is_current                as is_current
    , nullif(tms_root_id, '')   as tms_root_id
    , nullif(tms_series_id, '') as tms_series_id
    , total_seasons             as total_seasons
    , total_episodes            as total_episodes
    , root_show_id              as root_show_id
    , run_time                  as run_time
    , has_numbered_episodes     as has_numbered_episodes
    , program_type              as program_type
  from shows

)

select * from renamed
