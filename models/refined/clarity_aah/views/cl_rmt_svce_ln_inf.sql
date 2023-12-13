
    {% set clarity_table_name = "cl_rmt_svce_ln_inf" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

