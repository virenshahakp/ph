{%- macro normalize_id(field_name = "_id") -%}
  -- standardize base64 encoding values across our events to ensure strict base64 encoding
  case
  when mod(length( {{ field_name }} ), 4) = 0 -- MOD == 4 needs no adjustment
    then {{ field_name }}
  when mod(length( {{ field_name }} ), 4) = 2 -- MOD == 2 needs two more characters '=='
    then {{ field_name }}  || '=='
  when mod(length( {{ field_name }} ), 4) = 3 -- MOD == 3 needs one more character '='
    then {{ field_name }}  || '='
  else {{ field_name }}
end
{%- endmacro %}
