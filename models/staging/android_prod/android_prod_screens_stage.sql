{{
  config(
    materialized='view'
  )
}}

with

screens as (

  select * from {{ ref('android_prod_screens_source') }}

)

select * from screens