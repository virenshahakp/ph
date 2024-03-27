select
  impression_date
  , impressions
  , revenue
  , provider
from {{ ref('uploads_ad_revenue_historic_source') }}