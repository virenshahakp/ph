{{
  config(
    materialized='view'
  )
}}

with

screens as (

  select * from {{ ref('tvos_prod_screens_source') }}

)

select * from screens