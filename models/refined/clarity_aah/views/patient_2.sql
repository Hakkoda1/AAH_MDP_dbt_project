{% set clarity_table_name = "patient_2" %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}