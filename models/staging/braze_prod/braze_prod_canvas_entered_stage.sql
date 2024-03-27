with

canvas as (

  select * from {{ ref('braze_prod_canvas_entered_source') }}

)

select * from canvas
