
    {% set clarity_table_name = "cl_cmp_ot_incl_ranges" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

