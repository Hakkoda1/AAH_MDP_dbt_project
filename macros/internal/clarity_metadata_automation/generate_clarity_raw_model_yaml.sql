{% macro generate_clarity_raw_column_yaml(column, model, model_yaml, column_desc_dict, parent_column_name="") %}
    {% if parent_column_name %}
        {% set column_name = parent_column_name ~ "." ~ column.name %}
    {% else %}
        {% set column_name = column.name %}
    {% endif %}
    {% set column_metadata_query %}
        select column_id, column_name, data_type, description, nvl(ini, '') ini, nvl(item, '') item
        from {{ref('column_config')}}
        where table_name = REPLACE(UPPER('{{model}}'), 'RAW_') and column_name = REPLACE(UPPER('{{column.name}}'), 'VALUE_')
    {% endset %}
            
    {% set results = run_query(column_metadata_query) %}

    {% if execute %}
    {% set column_id = ','.join(results.columns[0].values()) %}
    {% set clarity_column_name = ','.join(results.columns[1].values()) %}
    {% set data_type = ','.join(results.columns[2].values()) %}
    {% set description = ','.join(results.columns[3].values()) %}
    {% set ini = ','.join(results.columns[4].values()) %}
    {% set item = ','.join(results.columns[5].values()) %}
    {% else %}
    {% set column_id = "" %}
    {% set clarity_column_name = "" %}
    {% set data_type = "" %}
    {% set description = "" %}
    {% set ini = "" %}
    {% set item = "" %}
    {% endif %}
    
    {% do model_yaml.append('      - name: ' ~ column.name | lower ) %}
    {% do model_yaml.append('        description: "' ~ description | replace('"','\\"') ~ '"') %}
    {% do model_yaml.append('        meta: ') %}
    {% do model_yaml.append('          column_id: ' ~ column_id) %}
    {% do model_yaml.append('          clarity_column_name: ' ~ clarity_column_name) %}
    {% do model_yaml.append('          data_type: ' ~ data_type) %}
    {% do model_yaml.append('          ini: ' ~ ini) %}
    {% do model_yaml.append('          item: ' ~ item) %}
    {% do model_yaml.append('') %}

    {% if column.fields|length > 0 %}
        {% for child_column in column.fields %}
            {% set model_yaml = codegen.generate_column_yaml(child_column, model_yaml, column_desc_dict, parent_column_name=column_name) %}
        {% endfor %}
    {% endif %}
    {% do return(model_yaml) %}
{% endmacro %}

{% macro generate_clarity_raw_model_yaml(model_names=[], upstream_descriptions=False) %}

    {% set model_yaml=[] %}

    {% do model_yaml.append('version: 2') %}
    {% do model_yaml.append('') %}
    {% do model_yaml.append('models:') %}

    {% if model_names is string %}
        {{ exceptions.raise_compiler_error("The `model_names` argument must always be a list, even if there is only one model.") }}
    {% else %}
        {% for model in model_names %}
            {% set table_metadata_query %}
                select distinct table_id, table_name, tbl_descriptor, load_frequency
                from {{ref('column_config')}}
                where table_name = REPLACE(UPPER('{{model}}'), 'RAW_')
            {% endset %}
            
            {% set results = run_query(table_metadata_query) %}

            {% if execute %}
            {# Return the first column #}
            {% set table_id = ','.join(results.columns[0].values()) %}
            {% set clarity_table_name = ','.join(results.columns[1].values()) %}
            {% set description = ','.join(results.columns[2].values()) %}
            {% set load_frequency = ','.join(results.columns[3].values()) %}
            {% else %}
            {% set table_id = "" %}
            {% set clarity_table_name = "" %}
            {% set description = "" %}
            {% set load_frequency = "" %}
            {% endif %}
            
            {% do model_yaml.append('  - name: ' ~ model | lower) %}
            {% do model_yaml.append('    description: ' ~ description) %}
            {% do model_yaml.append('    meta: ') %}
            {% do model_yaml.append('      table_id: '  ~ table_id) %}
            {% do model_yaml.append('      clarity_table_name: '  ~ clarity_table_name) %}
            {% do model_yaml.append('      load_frequency: '  ~ load_frequency) %}
            {% do model_yaml.append('    columns:') %}

            {% set relation=ref(model) %}
            {%- set relation=ref(model) -%}
            {%- set except = ["METADATA_FILENAME", "METADATA_FILE_ROW_NUMBER", "_DBT_COPIED_AT"] -%}
            {%- set columns = adapter.get_columns_in_relation(relation) -%}
            {% set column_desc_dict =  codegen.build_dict_column_descriptions(model) if upstream_descriptions else {} %}

            {% for col in columns %}
                {%- if col.column not in except -%}
                    {% set model_yaml = generate_clarity_raw_column_yaml(col, model, model_yaml, column_desc_dict) %}
                {%- endif %}
            {% endfor %}
        {% endfor %}
    {% endif %}

{% if execute %}

    {% set joined = model_yaml | join ('\n') %}
    {{ log(joined, info=True) }}
    {% do return(joined) %}

{% endif %}

{% endmacro %}