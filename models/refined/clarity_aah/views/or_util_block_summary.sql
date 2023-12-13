{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'or_util_block_summary' %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}
