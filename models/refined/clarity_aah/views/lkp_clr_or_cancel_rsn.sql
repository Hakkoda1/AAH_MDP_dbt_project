
    {% set clarity_table_name = "lkp_clr_or_cancel_rsn" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

