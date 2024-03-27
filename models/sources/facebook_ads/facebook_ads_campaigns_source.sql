with

campaigns as (

  select
    id     as campaign_id
    , received_at
    , uuid
    , name as campaign_name
    , spend_cap
    , account_id
    , effective_status
    , start_time
    , uuid_ts
    , buying_type
  from {{ source('facebook_ads', 'campaigns') }}

)

select * from campaigns