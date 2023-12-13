
    {% set clarity_table_name = "eap_assoc_cost_ctr" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

