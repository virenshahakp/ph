{% macro get_update_dates(default_lookback_days) -%}

{% set end_date = var("end_date") %}
{% set start_date = var("start_date") %}

{% if start_date == "" and end_date == "" %}

    {% set end_date = run_started_at.astimezone(modules.pytz.timezone("UTC")) %}
    {% set end_date_alias = end_date.strftime("%Y_%m_%d") %}
    {% set start_date = (end_date - modules.datetime.timedelta(default_lookback_days)) %}
    {% set end_date = end_date.date() %}
    {% set start_date = start_date.date() %}
  

{% else %}
   {% set end_date_alias = var("end_date") %}
{% endif %}


{% do return ({
        "start_date": start_date,
        "end_date": end_date,
        "end_date_alias" : end_date_alias
        })
    %}

{%- endmacro %}