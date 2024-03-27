{{ config(materialized='view') }}

/* this is for backwards compability only. It should not be used */

select * from {{ref('fct_beacons')}}