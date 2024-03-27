with

ads as (
  select
    id     as ad_id
    , received_at
    , uuid
    , utm_medium
    , bid_amount
    , url_parameters
    , name as campaign_name
    , status
    , utm_campaign
    , uuid_ts
    , adset_id
    , bid_type
    , utm_content
    , utm_term
    , account_id
    , campaign_id
    , utm_source
  from {{ source('facebook_ads', 'ads') }}

)

select * from ads