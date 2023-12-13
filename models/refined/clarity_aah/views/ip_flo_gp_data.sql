{% set clarity_table_name = "ip_flo_gp_data" %}
{% set except_columns = [] %}

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}