{%- macro detect_deprecated_columns(table_name) -%}
{% set tbl_name_upper = table_name | upper %}
{%set raw_database =  var("raw_database")%}
{% set deprecated_cols_query %}
select column_name from {{raw_database}}.general_mgmt.column_config
where table_name = '{{tbl_name_upper}}'
and deprecated_yn = 'Y'
{% endset %}

{% set results = run_query(deprecated_cols_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% do return(results_list)%}
{% else %}
{% set results_list = [] %}
{% do return(results_list)%}
{% endif %}




{%- endmacro -%}