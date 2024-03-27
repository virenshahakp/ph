with

benchmarks as (

  select * from {{ source('airbyte', 'quality_of_experience_benchmarks') }}

)

, renamed as (

  select
    asset_type
    , metric
    , metric_date
    , metric_period
    , platform
    , source
    , value as metric_value
  from benchmarks

)

select * from renamed
