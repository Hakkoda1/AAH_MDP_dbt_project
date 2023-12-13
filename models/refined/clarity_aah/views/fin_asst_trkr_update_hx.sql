
    {% set clarity_table_name = "fin_asst_trkr_update_hx" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

