
    {% set clarity_table_name = "hsd_base_class_map" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

