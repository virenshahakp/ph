with

costs as (

  select * from {{ ref('airbyte_costs_source') }}

)

select * from costs
