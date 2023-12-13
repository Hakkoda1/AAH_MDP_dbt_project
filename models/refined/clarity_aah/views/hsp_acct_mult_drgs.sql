
    {% set clarity_table_name = "hsp_acct_mult_drgs" %}

    {% set except_columns = [] %}

    {{ clarity_refined_view(clarity_table_name=clarity_table_name, except_columns=except_columns) }}
    

