
    {% set clarity_table_name = "cl_rmt_svc_lvl_adj" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

