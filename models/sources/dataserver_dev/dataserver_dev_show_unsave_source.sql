with

/*
 kenny dataservers were reporting events to dev schema for about 6 months.
 on 2018-09-10 the api key was changed back so prod data went to prod.
 This grabs the data that went into dev.
 */

unsaves as (

  select * from    {{- source('dataserver_dev','show_unsave') }}

)

, renamed as (

  select
    user_id
    , show_id
    , sent_at
    , received_at
    , "timestamp"
  from unsaves
  where
    unsaves."timestamp" < '2018-09-11'

)

select * from renamed