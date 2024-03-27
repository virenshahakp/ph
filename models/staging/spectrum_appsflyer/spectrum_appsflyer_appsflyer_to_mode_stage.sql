with

installs as (

  select * from {{ ref('spectrum_appsflyer_appsflyer_to_mode_source') }}

)

select * from installs