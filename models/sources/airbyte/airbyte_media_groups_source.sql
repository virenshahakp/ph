with

media_group as (

  select * from {{ source('airbyte', 'media_groups') }}

)

, renamed as (

  select
    callsign                          as callsign
    , name                            as channel_name
    , display_name                    as display_name
    , media_group                     as media_group
    , coalesce(on_philo = '1', false) as is_on_philo
    -- transformation:
    -- 1 remove the $
    -- replace all white space
    -- if empty string, then return null
    -- convert to number
    , to_number(
      nullif(
        replace(
          replace(
            case
              when rate2021 ilike '%n/a%' then ''
              else rate2021
            end
            , '$'
            , ''
          )
          , ' '
          , ''
        )
        , ''
      )
      , '99D99'
    )                                 as rate2021
    , to_number(
      nullif(
        replace(
          replace(
            case
              when rate ilike '%n/a%' then ''
              else rate
            end
            , '$'
            , ''
          )
          , ' '
          , ''
        )
        , ''
      )
      , '99D99'
    )                                 as rate
    , to_number(
      nullif(
        replace(
          replace(
            case
              when proposed_rate ilike '%n/a%' then ''
              else proposed_rate
            end
            , '$'
            , ''
          )
          , ' '
          , ''
        )
        , ''
      )
      , '99D99'
    )                                 as proposed_rate
  from media_group

)

select *
from renamed
where callsign is not null
