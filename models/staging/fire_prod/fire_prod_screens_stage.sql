{{
  config(
    materialized='view'
  )
}}

with

screens as (

  select * from {{ ref('fire_prod_screens_source') }}

)

select * from screens