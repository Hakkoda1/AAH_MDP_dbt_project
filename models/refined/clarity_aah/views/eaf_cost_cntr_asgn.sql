
    {% set clarity_table_name = "eaf_cost_cntr_asgn" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

