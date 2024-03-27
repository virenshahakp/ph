with

source as (

  select * from {{ source('guide', 'recordings') }}

)

, renamed as (

  select
    id                        as recording_id
    , 'RECORDING'::varchar(9) as asset_type
    , broadcast_id            as broadcast_id
    , duration_over_record    as run_time
    , created_at::date        as created_at
    , {{ normalize_id("_id") }}                           as asset_id
  from source

)

select * from renamed
