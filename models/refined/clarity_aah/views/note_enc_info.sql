{{ config(
    tags=["not_enabled_uat"]
) }}
{% set clarity_table_name = "note_enc_info" %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}