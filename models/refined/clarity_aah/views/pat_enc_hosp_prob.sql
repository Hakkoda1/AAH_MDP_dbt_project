{% set clarity_table_name = "pat_enc_hosp_prob" %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}