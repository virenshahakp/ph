
{{ config(
    materialized='table',
    alias='customer123')
}}

select * from {{ source('my_project','CUSTOMER') }} 