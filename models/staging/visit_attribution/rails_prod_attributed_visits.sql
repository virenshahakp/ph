WITH 

rails_prod AS (

  SELECT * FROM {{ ref('rails_prod_pages_stage') }}

)

, web_prod_pages AS (

  SELECT * FROM {{ ref('web_prod_pages_stage') }}
  
)

SELECT DISTINCT 
  rails_prod.* 
FROM rails_prod
JOIN web_prod_pages ON (rails_prod.anonymous_id = web_prod_pages.anonymous_id)

/*
A pattern of WHERE EXISTS could be used in some databases instead of the 
INNER JOIN + DISTINCT on this query and WHERE EXISTS would be more performant
But redshift is unable to compile it given our nested dbt views as it becomes a
correlated subquery.

example:

WHERE EXISTS (
  SELECT 1 
  FROM web_prod_pages 
  WHERE rails_prod.anonymous_id = web_prod_pages.anonymous_id
)

Comment from migrated periscope view as to why we include rails_prod data in our
attributed visits:
A campaign that we ran with AMC was using the go link /guhhatl before it was
ready.  So to help attribute this data, we have to get these visits from the
server side try logs.

AP: TODO: I do not understand the above comment. Why do we need to inner join
    on web_prod.pages?
LB: the inner join ensures that the user did actually load the web page and it was not 
some fake traffic.
*/
