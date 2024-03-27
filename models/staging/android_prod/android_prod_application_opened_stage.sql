with

opened as (

  select * from {{ ref('android_prod_application_opened_source') }}

)

select *
from opened
