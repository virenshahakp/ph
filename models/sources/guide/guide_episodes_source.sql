with

episodes as (

  select * from {{ source('guide', 'episodes') }}

)

, renamed as (

  select
    id                    as episode_id
    , episode_num         as episode_num
    , season_num          as season_num
    , orig_air_date::date as original_air_date
    , created_at::date    as created_at
    , {{ normalize_id("_id") }}                       as asset_id
    , lower(title)        as show_title
    , lower(subtitle)     as episode_title
  from episodes

)

select * from renamed
