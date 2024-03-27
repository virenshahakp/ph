with

source as (

  select * from {{ source('guide', 'vod_assets') }}

)

, renamed as (

  select
    id                    as vod_asset_id
    , 'VOD'::varchar(9)   as asset_type
    , channel_id          as channel_id
    , show_id             as show_id
    , episode_id          as episode_id
    , license_start::date as license_start
    , license_end::date   as license_end
    , run_time            as run_time
    , entity_type         as content_type
    , {{ normalize_id("_id") }}                       as asset_id
  from source

)

select * from renamed
