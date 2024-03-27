with 

package as (

  select * from {{ ref('rails_prod_package_added_source') }}

)

select * from package