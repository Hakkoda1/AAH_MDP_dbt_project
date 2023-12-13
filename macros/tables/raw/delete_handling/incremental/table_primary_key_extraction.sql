{% macro table_primary_key_extraction ( table ) %}

    {# This is an auxiliary macro that would extract the primary keys for a given table #}

    {% set pk_query %} 
        SELECT lower(column_name) AS primary_key 
        FROM {{ ref('column_config') }} 
        WHERE table_name = upper( '{{table}}' ) AND primary_key = 'TRUE' 
    {% endset %}

    {% set results = run_query(pk_query) %}
    {% set pk_list=[] %}

    {% if execute %}
        {% set column_list = results.columns[0].values() %}
    {% else %}
         {% set column_list = [] %}
    {% endif %}
    
    {% for column in zip(column_list) %}
        {% do pk_list.append( column | lower | replace('(','') | replace(",","") | replace(')','') | replace("'","") ) %}
    {% endfor %}

    {% do return(pk_list) %}

{% endmacro %}