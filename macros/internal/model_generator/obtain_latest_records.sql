{% macro obtain_latest_records(model,tbl_name) %}

{%- set table_column_list_query -%}
select 
    column_name
from {{ model }} src
where table_name = '{{ tbl_name }}' and primary_key = true
order by pk_line asc
{%- endset -%}

{%- set results = run_query(table_column_list_query) -%}

{%- if execute -%}
{%- set column_list = results.columns[0].values() -%}
{%- else -%}
{%- set column_list = [] -%}
{%- endif -%}


row_number() over (partition by

{%- for column in column_list -%}
    {%- if loop.last -%}
        {{" " + column + " "}}
    {%- else -%}
        {{" "+ column + ", "}}
    {%- endif -%}
{%- endfor -%}
 order by azure_upload_date desc)

{% endmacro %}