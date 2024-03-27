with

benchmarks as (

  select * from {{ ref('airbyte_quality_of_experience_benchmarks_source') }}

)

select * from benchmarks
