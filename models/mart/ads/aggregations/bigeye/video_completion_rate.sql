{{ config(materialized='view') }}

select
  asset_type
  , partition_date
  , client_type
  , channel
  , count(1)::numeric                                                           as inserted_ad_count
  , sum(nullif(impression_count, 0))::numeric                                   as impressions
  , sum(nullif(complete_count, 0))::numeric                                     as completed_ad_impressions
  , sum(unique_impression_count)::numeric                                       as unique_impression_count
  , sum(unique_complete_count)::numeric                                         as unique_complete_count
  , sum(impression_count)::numeric / count(1)::numeric                          as insertion_to_impression_rate
  , sum(complete_count * 1.0) / sum(nullif(impression_count * 1.0, 0))::numeric as video_completion_rate
from {{ ref('fct_vast_ads_enriched') }}
where is_inserted is true
{{ dbt_utils.group_by(n=4) }}