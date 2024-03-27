with

source as (

  select * from {{ source('rails_prod', 'users') }}

)

, renamed as (

  select
    id                                   as user_id
    , roles                              as roles
    , subscriber_billing                 as subscriber_billing
    , subscriber_state                   as subscriber_state
    , created_at                         as created_at
    , labels                             as labels
    , packages                           as packages
    , dma_code                           as dma_code
    , dma_region                         as dma_region
    , dma_name                           as dma_name
    , income                             as income
    , age_range                          as age_range
    , gender                             as gender
    , referrer_id                        as referrer_id
    , referral_type                      as referral_type
    , has_email                          as has_email
    , has_phone                          as has_phone
    , direct_billed                      as is_direct_billed
    , signup_source                      as signup_source
    , uuid_ts                            as loaded_at
    -- some user accounts created before profiles were released have a null root_user_id
    -- these accounts are also root user's    
    , coalesce(
      root_user_id
      , id
    )                                    as account_id
    , coalesce(id = root_user_id, false) as is_root
    -- only allow integer zip codes
    , case
      when regexp_count(zip, '^[0-9]+$') > 0
        then zip
    end                                  as zip
  from source

)

select * from renamed
