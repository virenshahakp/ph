with

source as (

  select * from {{ ref('android_prod_identifies_source') }}

)

select * from source
