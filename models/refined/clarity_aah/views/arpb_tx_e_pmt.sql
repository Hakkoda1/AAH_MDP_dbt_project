
    {% set clarity_table_name = "arpb_tx_e_pmt" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

