with

unsaves as (

  select * from {{ source('periscope_views', 'initial_unsaves') }}

)

, renamed as (

  select
    created_at             as received_at
    , created_at           as sent_at
    -- adjust to match unsave event structure
    , created_at           as "timestamp"
    , user_id::varchar(36) as user_id
    , show_id::varchar(20) as show_id
  from unsaves

)

select * from renamed
