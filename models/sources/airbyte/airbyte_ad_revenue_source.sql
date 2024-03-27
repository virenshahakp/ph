/*
 * this model captures the ad_revenue by source for the current calendar year
 * historical data is fixed and loaded into the warehouse as a static table
 * uploads.ad_revenue_historic
 *
 * at the end of each year, once revenue is finalized data should be migrated
 * to the _historic table so that the ingestion will be more stable
 */
with

provider as (

  select * from {{ source('airbyte', 'ad_revenue') }}

)

, renamed as (

  select
    date::date as impression_date
    , {{- try_cast_numeric(
        'impressions', 
        'decimal', 
        'FM9G999G999D99') 
      -}}                  as impressions
    , {{- try_cast_numeric(
        'revenue', 
        'decimal', 
        'FM9G999G999D99') 
      -}}                  as revenue
    , provider as provider
  from provider
  where
    revenue is not null
    and trim(revenue) != ''
    and {{ try_cast_numeric(
        'revenue', 
        'decimal', 
        'FM9G999G999D99') 
      -}} > 0
    and impressions is not null
    and trim(impressions) != ''
    and {{ try_cast_numeric(
        'impressions',
        'decimal',
        'FM9G999G999D99') 
      -}} > 0

)

select * from renamed
