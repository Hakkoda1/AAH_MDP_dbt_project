{% macro insert_default_key_to_dim_table( model_name, primary_key ) %}

{# Takes input metadata for Model name to generate and execute the insert statement of a default record to a table. #}
    
    {% set default_value = '-1' %}
    {% set database_name = 'MDP_CONFORMED_' ~ target.name %}
    {% set schema = 'ENTERPRISE_MASTER_BASE' %}
    
    {% set pk_query %}

        select column_name pk_name
        from {{ database_name }}.information_schema.columns 
        where table_schema = '{{ schema }}'
            and lower( table_name ) = lower('{{ model_name }}') 
            and upper( column_name ) like '%_KEY' 
            and ordinal_position = 1
    
    {% endset %}

    {% set results = run_query(pk_query) %}

    {% if execute %}
        {# Return the first column #}
        {% set pk_list = results.columns[0].values() %}
    {% else %} {% set pk_list = [] %}
    {% endif %}

    insert into {{ database_name }}.{{ schema }}.{{ model_name }} (
            {% for pk_name in pk_list %}
                {{ pk_name }}
            {% endfor %},
            mdp_effective_date, 
            record_source
            )

        with default_record as (
            select 
                {{ dbt_utils.generate_surrogate_key( [ default_value ] ) }} {{ primary_key }},
                to_timestamp_ltz( '1900-01-01' ) mdp_effective_date, 
                'SYSTEM' record_source
            )

        select *
        from default_record s
        where not exists (
            select *
            from {{ database_name }}.{{ schema }}.{{ model_name }} S2
            where S.{{ primary_key }} = S2.{% for pk_name in pk_list %}{{ pk_name }}{% endfor %}
            )

{% endmacro %}