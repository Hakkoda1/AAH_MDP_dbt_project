{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = 'hsp_acct_extinj_cd' %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}
