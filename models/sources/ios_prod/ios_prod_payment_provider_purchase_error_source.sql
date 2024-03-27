with

payment_provider_purchase_error as (

  select * from {{ source('ios_prod', 'payment_provider_purchase_error') }}

)

, renamed as (

  select
    {{ apple_common_columns() }}
    , sku
    , provider_name
    , code
  from payment_provider_purchase_error

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}