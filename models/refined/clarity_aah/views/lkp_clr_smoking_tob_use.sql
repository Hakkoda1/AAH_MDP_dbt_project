{% set clarity_table_name = 'lkp_clr_smoking_tob_use' %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}
