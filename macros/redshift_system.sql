{%- macro include_redshift_prod_schemas() -%}
(
  (
    "schema" in ('analytics', 'dbt_staging', 'dbt_sources')                     -- dbt schemas
    or "schema" in ('adverity', 'airbyte', 'qualaroo', 'rudderstack')           -- saas vendor schemas
    or "schema" in ('appsflyer', 'demographics', 'guide', 'publica', 'zendesk') -- regular load source schemas
    or "schema" in ('facebook_ads', 'google_ads')                               -- regular load source schemas (continued)
    or "schema" in ('derived', 'periscope_views', 'uploads')                    -- ad hoc upload schemas
    or "schema" like '%\_prod'                                                  -- prod source schemas
  )
  -- exclude temp and system tables within these schemas --
  and "table" not like 'temp\_%'
  and "table" not like '\_airbyte%'
)
{%- endmacro -%}