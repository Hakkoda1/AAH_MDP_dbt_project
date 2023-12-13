
    {% set clarity_table_name = "x_bfg_rltd_fee_sched" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

