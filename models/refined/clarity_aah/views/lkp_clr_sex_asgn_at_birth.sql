{% set clarity_table_name = "lkp_clr_sex_asgn_at_birth" %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}