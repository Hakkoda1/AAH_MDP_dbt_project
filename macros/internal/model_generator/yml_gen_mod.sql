-- depends_on: {{ ref('column_config') }}
-- yml_col_gen('Clarity_POC',['LIST of values to exclude'])
{% macro yml_col_gen_mod(table,model_yaml) %}

    
    {% set column_metadata_query %}
        select 
            table_name,
            column_name,
            description
        from {{ref('column_config')}}
        where
            table_name = replace(upper('{{ table }}'), 'LKP_CLR' , 'ZC' ) OR table_name = upper('{{ table }}')
        order by line, column_name
    {% endset %}

    {% set results = run_query(column_metadata_query) %}

    {% if execute %}
    {%- set column_list = results.columns[1].values() -%}
    {%- set description_list = results.columns[2].values() -%}
    {%- endif -%}

    {% do model_yaml.append('   columns:') %}
    {% for (column,description) in zip(column_list,description_list) %}
        {% do model_yaml.append('      - name: ' ~ column | lower) %}
        {% if description == '' or description == 'null' or description == None %}
        {% else %}
            {% do model_yaml.append('        description: "' ~ description | replace('"','\\"') | replace(":","=")  ~ '"') %}
        {% endif %}

    {%- endfor -%}

    {% do return(model_yaml) %}
{% endmacro %}


{% macro yml_gen_mod(tables_location_sf,include_list) %}

    {% set model_yaml=[] %}

    {% set refined_models_name %}
    show views in schema {{tables_location_sf}}
    {% endset %}

    {% set results = run_query(refined_models_name) %}
    
    {% if execute %}
    {%- set refined_models_list = results.columns[1].values() -%}
    {%- endif -%}

    {% do model_yaml.append('version: 2') %}
    {% do model_yaml.append('') %}
    {% do model_yaml.append('models:') %}

        {# {%- for table in table_list -%} #}
        {% for table in refined_models_list %}
            {% if table in include_list %}
                {% do model_yaml.append('') %}
                {% do model_yaml.append(' - name: ' ~ table | lower) %}
                {% do model_yaml.append('   config: ' ) %}
                {% do model_yaml.append('     tags: []' ) %}
                {% do model_yaml.append('   tests: ' ) %}
                {% do model_yaml.append('      - check_clarity_row_count_against_source' ) %}
                {% set model_yaml = yml_col_gen_mod(table,model_yaml) %}
            {%- endif -%}
        {%- endfor -%}

        {% if execute %}

            {% set joined = model_yaml | join ('\n') %}
            {{ log(joined, info=True) }}
            {% do return(joined) %}

        {% endif %}
      

    --Loop through each table and fill in parameters
    /*
- name: clarity_emp
   config: 
     tags: []
   tests: 
      - check_clarity_row_count_against_source
   columns:
      - name: user_id
        description: "The unique ID assigned to the user record. This ID may be encrypted."
      - name: name
        description: "The name of the user record. This name may be hidden."
      - name: prov_id
        description: "The unique ID of the provider record that is linked to this user record if the user is a clinical system provider. This ID may be encrypted."
      - name: epic_emp_id
        description: "The unique ID assigned to the user record. This column will be omitted from public views of the CLARITY_EMP table."
      ...

    */
{% endmacro %}