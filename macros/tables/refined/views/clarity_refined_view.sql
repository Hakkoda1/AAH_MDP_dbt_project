{# Takes an input clarity table name and columns that should be removed from the refined layer for a given clarity table and generates sql for the refined layer. #}
{% macro clarity_refined_view(clarity_table_name, except_columns = []) %}
{%- set except_columns = detect_deprecated_columns(clarity_table_name) -%}
{% set default_except_column = ["deleted_yn"] %}
{% set except_columns_with_additions = except_columns | list + default_except_column %}
select {{ dbt_utils.star(from=ref(clarity_table_name~"_base"), except=except_columns_with_additions, quote_identifiers=False) }}
from {{ref(clarity_table_name~"_base")}}
where deleted_yn = 'N'
{% endmacro %}