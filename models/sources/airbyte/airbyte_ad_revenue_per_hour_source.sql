with

revenue as (

  select * from {{ source('airbyte', 'rev_per_hour') }}

)

, renamed as (

  select
    date::date as event_date
    , {{- try_cast_numeric(
        'rev_per_hour', 
        'decimal', 
        '999D99') 
      -}}                    as ad_rev_per_hour
  from revenue
  where
    revenue is not null
    and trim(revenue) != ''
    and date is not null
  order by 1

)

select * from renamed
