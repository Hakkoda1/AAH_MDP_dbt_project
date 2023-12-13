{% macro retrieve_col_names(model,tbl_name,append) %}

{%set replace_underscore = var("replace_underscore") %}
{%set add_suffix = var("add_suffix") %}
{%set suffix = var("suffix") %}
{%set add_prefix =  var("add_prefix")%}
{%set prefix =  var("prefix")%}


{%- set table_column_list_query -%}
select 
    TABLE_NAME, --0
    upper(COLUMN_NAME), --1
    LOAD_FREQUENCY, --2
    INI, --3
    case  --4
        when data_type is null 
            then 'VARCHAR'
        when data_type = 'DATETIME' then
            (case 
                when hour_format = 'DATE ONLY' then
                    'TIMESTAMP'
                when hour_format = 'DATETIME 24HR INCL SECONDS' then
                    'TIMESTAMP'
                when hour_format = 'DATETIME 12HR' then
                    'TIMESTAMP'
                when hour_format = 'TIME ONLY 24HR' then
                    'TIME'
                else 
                    'TIMESTAMP' end)
        when clarity_precision is not null
            then data_type || '(' || clarity_precision || ')'
        when clarity_precision is null
            then data_type
        end as data_type_input
from {{ model }} src
where table_name = '{{ tbl_name }}'
order by line, column_name
{%- endset -%}

{%- set results = run_query(table_column_list_query) -%}

{%- if execute -%}
{%- set column_list = results.columns[1].values() -%}
{%- set data_type_list = results.columns[4].values() -%}
{%- else -%}
{%- set column_list = [] -%}
{%- set data_type_list = [] -%}
{%- endif -%}

{% for (column,data_type) in zip(column_list,data_type_list) %}
    {%- if loop.last %}
        {{ append + column +  '::' + data_type + ' as ' + rename_column(column) }}
    {%- else %}
        {{ append + column + '::' + data_type + ' as ' + rename_column(column) }},
    {%- endif -%}
{%- endfor -%}

{% endmacro %}