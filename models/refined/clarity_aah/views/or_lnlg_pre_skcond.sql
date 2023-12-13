
    {% set clarity_table_name = "or_lnlg_pre_skcond" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

