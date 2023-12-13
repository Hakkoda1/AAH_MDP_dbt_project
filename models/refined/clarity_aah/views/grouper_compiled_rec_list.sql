
    {% set clarity_table_name = "grouper_compiled_rec_list" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

