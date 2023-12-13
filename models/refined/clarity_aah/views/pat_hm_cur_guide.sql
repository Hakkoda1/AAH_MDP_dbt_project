{% set clarity_table_name = 'pat_hm_cur_guide' %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}