with

payment_provider_purchase_start as (

  select * from {{ source('fire_tv_prod', 'payment_provider_purchase_start') }}

)

, renamed as (

  select
    {{ android_common_columns() }}
    , sku
    , provider_name
  from payment_provider_purchase_start

)

select *
from renamed
{%- if target.name != 'prod' %}
  where received_at >= {{ dbt.dateadd('day', -incremental_dev_mode_days(), 'current_date') }}
{%- endif -%}
