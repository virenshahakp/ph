with

source as (

  select * from {{ source('guide', 'channels') }}

)

, renamed as (

  select
    id                      as channel_id
    , 'CHANNEL'::varchar(9) as asset_type
    , {{ normalize_id("_id") }}                         as asset_id
    , callsign              as callsign
    , name                  as channel_name
    , has_public_view       as has_public_view
    , is_premium            as is_premium
    , is_free               as is_free
  from source

)

select * from renamed
