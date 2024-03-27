{{
  config(
    materialized='view'
  )
}}

with

screens as (

  select * from {{ ref('ios_prod_screens_source') }}

)

select * from screens