
    {% set clarity_table_name = "ed_iev_linked_order_info" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

