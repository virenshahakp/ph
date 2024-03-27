with

source as (

  select
    experiment_id
    , experiment_name
    , experiment_concluded_at
  from {{ ref('export_experiments_source') }}

)

select * from source