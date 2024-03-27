{{
  config(
    materialized='view'
  )
}}

with

screens as (

  select * from {{ ref('roku_prod_screens_source') }}

)

select * from screens