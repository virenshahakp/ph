with

experiments as (

  select
    id             as experiment_id
    , short_name   as experiment_name
    , concluded_at as experiment_concluded_at
  from {{ source('export','experiments') }}

)

select * from experiments