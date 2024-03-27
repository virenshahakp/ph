with

identifies as (

  select * from {{ ref('web_prod_identifies_source') }}

)

select * from identifies
