with

source as (

  select * from {{ source('guide', 'broadcasts') }}

)

, renamed as (

  select
    id                      as broadcast_id
    , 'CHANNEL'::varchar(9) as asset_type
    , tribune_is_premiere   as is_premiere
    , tribune_is_new        as is_new
    , channel_id            as channel_id
    , show_id               as show_id
    , episode_id            as episode_id
    , starts_at             as starts_at
    , ends_at               as ends_at
    , has_dvs               as has_audio_description
    , {{ normalize_id() }}                         as asset_id
    , {{ dbt.datediff('starts_at', 'ends_at', 'seconds') }}                   as run_time
  from source

)

select * from renamed
