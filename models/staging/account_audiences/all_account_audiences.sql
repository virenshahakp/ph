/*
  The account_audience_* models need to be run and exist in the database before this model will incorporate them.

  That is, we don't have ref() commands in this file and as such the model is assumed to be independent of the DAG.
  Unfortunately that is a side-effect of programmatically building the account_audiences at this time with the dbt_util
  get_relations_by_pattern() method.

  Fortunately the all_account_audiences view will only ever be behind by one run. So it can either be configured to
  run after the account_audiences are built or it will simply lag one run behind in terms of incorporating new account
  audiences. Since it is a view any changed logic from existing audiences will be immediately incorporated.

  Unfortunately, it also means to deprecate an account_audience we need to explicitly drop the view from redshift.

*/
{% set staging_schema = 'dbt_staging' %}
{% set account_audience_models = dbt_utils.get_relations_by_pattern(staging_schema, 'account_audience_%') %}

with

all_audiences as (

  {{ dbt_utils.union_relations(relations = account_audience_models) }}


)

select distinct * from all_audiences