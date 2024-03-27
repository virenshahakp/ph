with

email as (

  select * from {{ ref('braze_prod_email_sent_source') }}

)

select * from email
