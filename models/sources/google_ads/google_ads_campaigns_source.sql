with

campaigns as (

  select
    id     as campaign_id
    , received_at
    , uuid
    , name as campaign_name
    , end_date
    , serving_status
    , start_date
    , status
    , uuid_ts
    , adwords_customer_id
  from {{ source('google_ads', 'campaigns') }}

)

select * from campaigns