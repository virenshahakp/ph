{{
  config(
    materialized='view'
  )
}}

with

screens as (

  select * from {{ ref('androidtv_prod_screens_source') }}

)

select * from screens