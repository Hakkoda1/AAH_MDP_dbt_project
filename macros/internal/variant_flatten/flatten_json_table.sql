{% macro flatten_json_table(model_name) %}
{# Flattens all fields containing unstructured data types (obj, arrays, variant). 
Generates a new field per each full json path.#}

{%- set new_columns_list = [] -%}
{%- set json_columns_list = [] -%}

{# each json_column is an agate column object#}
{%- for json_column in adapter.get_columns_in_relation(model_name) -%}

    {%- if json_column.data_type in ('VARIANT', 'ARRAY', 'OBJECT') -%}

        {{ json_columns_list.append(json_column) or "" }}

    {% endif -%}
{% endfor -%}

{%- for json_column in json_columns_list -%}

    {%- call statement('get_json_path', fetch_result=True) -%}

        {{ get_json_path(model_name, json_column.column) }}

    {%- endcall -%}

    {%- set path_results = load_result('get_json_path') -%}

    {# Tuple of paths given as strings#}
    {%- set json_path = path_results["table"].columns[0].values() -%}
    {# Data type is needed for correct concatenation#}
    {%- call statement('get_data_type', fetch_result=True) -%}

         select 
            typeof( {{json_column.column}} ) from {{model_name}} 
            where typeof( {{json_column.column}} ) is not null
            limit 1

    {% endcall -%}
{# If nesting level is 0, no path is needed#}
    {% if load_result('get_data_type')['data'] != [] -%}

        {%- set data_type = load_result('get_data_type')['data'][0][0] %}

    {% else -%}

        {{json_columns_list.remove(json_column) or ""}}

    {% endif -%}
       
{# Build  new field name as concatenation#}
    {%- for values in json_path -%}
        {%- if data_type in 'ARRAY' -%}
                {{ new_columns_list.append(json_column.column ~ values) or "" }}
        {%- elif data_type in 'OBJECT' -%} 
                {{ new_columns_list.append(json_column.column ~ ":" ~ values) or "" }}
        {% else %}     
                {{ new_columns_list.append(json_column.column) or "" }}
            {% endif -%}
    {% endfor -%}
{% endfor -%}


select
    {% for json_path in new_columns_list -%}
    {{ json_path }}
    as {{
        json_path | replace(".", "_") | replace("[", "") | replace("]", "") | replace("'", "") | replace(":", "_")
    }},
    {% endfor -%}
    * 
    {# Excludes flattened fields to avoid repetition#}
    exclude (
        {% for col in json_columns_list -%}
                {{ col.column }}{{ ", " if not loop.last else "" }}
        {% endfor -%}
    )
from {{ model_name }}

{% endmacro %}