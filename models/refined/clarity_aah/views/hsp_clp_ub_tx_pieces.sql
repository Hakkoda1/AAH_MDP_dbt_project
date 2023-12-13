
    {% set clarity_table_name = "hsp_clp_ub_tx_pieces" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

