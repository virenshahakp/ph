{%- macro neustar_1418_household_composition(composition) -%}
case when {{ composition }} is null or {{ composition }} = '' then null
     when {{ composition }} = 'A' then '1 adult female'
     when {{ composition }} = 'B' then '1 adult male'
     when {{ composition }} = 'C' then '1 adult female and 1 adult male'
     when {{ composition }} = 'D' then '1 adult female, 1 adult male and children'
     when {{ composition }} = 'E' then '1 adult female and children present'
     when {{ composition }} = 'F' then '1 adult male and children present'
     when {{ composition }} = 'G' then '2 or more adult males'
     when {{ composition }} = 'H' then '2 or more adult females'
     when {{ composition }} = 'I' then '2 or more adult males and children'
     when {{ composition }} = 'J' then '2 or more adult females and children'
     else 'Unknown'
end
{%- endmacro -%}

{%- macro neustar_1418_homeowner(homeowner) -%}
  case when {{ homeowner }} is null or {{ homeowner }} = '' then null
    when {{ homeowner }} = 'H' then 'Homeowner'
    when {{ homeowner }} = '9' then 'Extremely Likely'
    when {{ homeowner }} = '8' then 'Highly Likely'
    when {{ homeowner }} = '7' then 'Likely'
    when {{ homeowner }} = 'R' then 'Renter'
    when {{ homeowner }} = 'T' then 'Probable Renter'
    when {{ homeowner }} = 'F' then 'Family Member'
    else 'Unknown'
  end
{%- endmacro -%}

{%- macro neustar_1418_education(education) -%}
  case when {{ education }} is null or {{ education }} = '' then null
       when {{ education }} = '11' then 'HS Diploma - Extremely Likely'
       when {{ education }} = '12' then 'Some College - Extremely Likely'
       when {{ education }} = '13' then 'Bach Degree - Extremely Likely'
       when {{ education }} = '14' then 'Grad Degree - Extremely Likely'
       when {{ education }} = '15' then 'Less than HS Diploma - Ex Like'
       when {{ education }} = '16' then 'Doctorate Degree - Extremely Likely'
       when {{ education }} = '51' then 'HS Diploma - Likely'
       when {{ education }} = '52' then 'Some College - Likely'
       when {{ education }} = '53' then 'Bach Degree - Likely'
       when {{ education }} = '54' then 'Grad Degree - Likely'
       when {{ education }} = '55' then 'Less than HS Diploma - Likely'
       when {{ education }} = '56' then 'Doctorate Degree - Likely'
       else 'Unknown'
  end
{%- endmacro -%}
