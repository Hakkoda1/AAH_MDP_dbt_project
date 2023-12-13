
    {% set clarity_table_name = "f_pat_mychart_status_hx" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

