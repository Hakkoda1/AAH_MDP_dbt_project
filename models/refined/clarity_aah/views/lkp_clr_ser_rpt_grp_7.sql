
    {% set clarity_table_name = "lkp_clr_ser_rpt_grp_7" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

