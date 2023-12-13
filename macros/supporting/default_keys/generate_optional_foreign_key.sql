{% macro generate_optional_foreign_key( column_name ) %}

{# Takes the column name and verify if the column has a null value,
   if it does returns the hash value of '-1',
   if not returns the hash value of the column
#}

    {% set default_value = dbt_utils.generate_surrogate_key( [ '-1' ] ) %}
    {% set column_value = dbt_utils.generate_surrogate_key( [ column_name ] ) %}

    IFF( 
        {{ column_name }} is null, 
        {{ default_value }},
        {{ column_value }} 
       )

{% endmacro %}