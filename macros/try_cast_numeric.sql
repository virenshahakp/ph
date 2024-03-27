{%- macro try_cast_numeric(str, datatype, format) -%}

  {%- if datatype == 'bigint' or datatype == 'int' -%}

    case
      when trim({{str}}) ~ '^[0-9]+$'
      then trim({{str}})
    end::{{datatype}}

  {%- elif datatype == 'decimal' -%}

    {%- if format is defined and format|length -%}

      case
        when trim({{str}}) = ''
        then null
        else to_number(trim({{str}}), '{{format}}')
      end

    {% else %}
      
      {{ exceptions.raise_compiler_error(
        "a format string is required for decimal conversions") }}

    {% endif %}

  {% else %}

    {{ exceptions.raise_compiler_error(
        "non-integer and non-decimal datatypes are not currently supported") }}

  {% endif %}

{%- endmacro -%}