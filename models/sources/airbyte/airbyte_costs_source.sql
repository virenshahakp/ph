with

costs as (

  select * from  {{- source('airbyte', 'costs') }}

)

, remove_non_numeric as (

  select
    quarter                              as quarter
    , month::date                        as month  -- noqa: L029
    -- transformation
    -- 1: remove dollar sign
    , ending_subscribers                 as ending_subscribers
    , replace(aws, '$', '')              as aws_costs_per_user
    , replace(taskus, '$', '')           as taskus_costs_per_user
    , replace(marketing_spend, '$', '')  as marketing_costs_total
    , replace(cac, '$', '')              as customer_acquisition_costs_per_user
    , replace(ad_revenue, '$', '')       as ad_revenue
    , replace(content_costs, '$', '')    as content_costs
    , replace(ad_costs, '$', '')         as ad_costs
    , replace(variable_costs, '$', '')   as variable_costs
    , replace(platform_costs, '$', '')   as platform_costs
    , replace(fastly_edgecast, '$', '')  as fastly_edgecast
    , replace(stripe_chargebee, '$', '') as stripe_chargebee
    , replace(gracenote, '$', '')        as gracenote
  from costs

)

, renamed as (

  select
    quarter
    , month -- noqa: L029
    -- transformation
    -- 2: convert to number
    ,        {{- try_cast_numeric('aws_costs_per_user', 'decimal', 'FM9G999G999D99') -}}                                  as aws_costs_per_user
    ,        {{- try_cast_numeric('taskus_costs_per_user', 'decimal', 'FM9G999G999D99') -}}                               as taskus_costs_per_user
    ,        {{- try_cast_numeric('marketing_costs_total', 'decimal', 'FM9G999G999D99') -}}                               as marketing_costs_total
    ,              {{- try_cast_numeric('customer_acquisition_costs_per_user', 'decimal', 'FM9G999G999D99') -}}                     as customer_acquisition_costs_per_user
    ,        {{- try_cast_numeric('ad_revenue', 'decimal', 'FM9G999G999D99') -}}                                          as ad_revenue
    ,        {{- try_cast_numeric('content_costs', 'decimal', 'FM9G999G999D99') -}}                                       as content_costs
    ,        {{- try_cast_numeric('ad_costs', 'decimal', 'FM9G999G999D99') -}}                                            as ad_costs
    ,        {{- try_cast_numeric('variable_costs', 'decimal', 'FM9G999G999D99') -}}                                      as variable_costs
    ,        {{- try_cast_numeric('platform_costs', 'decimal', 'FM9G999G999D99') -}}                                      as platform_costs
    ,        {{- try_cast_numeric('fastly_edgecast', 'decimal', 'FM9G999G999D99') -}}                                     as fastly_edgecast
    ,        {{- try_cast_numeric('stripe_chargebee', 'decimal', 'FM9G999G999D99') -}}                                    as stripe_chargebee
    ,        {{- try_cast_numeric('gracenote', 'decimal', 'FM9G999G999D99') -}}                                           as gracenote
    ,        {{- try_cast_numeric('ending_subscribers', 'decimal', 'FM9G999G999D99') -}}                                  as ending_subscribers
  from remove_non_numeric

)

select * from renamed
