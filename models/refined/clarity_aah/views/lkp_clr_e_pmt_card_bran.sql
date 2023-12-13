{% set clarity_table_name = "lkp_clr_e_pmt_card_bran" %}
{% set except_columns = [] %} 

{{ clarity_refined_view(clarity_table_name=clarity_table_name,except_columns=except_columns) }}